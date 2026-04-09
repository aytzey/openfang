'use strict';

import { OpenFangAPI, OpenFangToast } from '../../core/api.js';

export const salesDataMixins = {
    async init() {
      var self = this;
      this.currentPage = this.routePage();
      this.syncTabFromHash();
      window.addEventListener('beforeunload', function() {
        self.stopJobPolling();
      });
      window.addEventListener('hashchange', function() {
        var nextPage = self.routePage();
        var pageChanged = nextPage !== self.currentPage;
        self.currentPage = nextPage;
        self.syncTabFromHash();
        if (pageChanged) {
          self.stopJobPolling();
          self.currentJobId = '';
          self.jobProgress = null;
          self.profileBrief = '';
          self.profile = self.normalizeProfile({});
          self.selectedRunId = '';
          self.selectedProspectId = '';
          self.selectedDossier = null;
          self.runs = [];
          self.prospects = [];
          self.runProspects = [];
          self.leads = [];
          self.runLeads = [];
          self.approvals = [];
          self.deliveries = [];
          self.refreshAll();
        }
      });
      window.addEventListener('keydown', function(evt) {
        self.handleApprovalHotkeys(evt);
      });
      window.addEventListener('message', function(evt) {
        var data = evt && evt.data ? evt.data : {};
        if (data && data.type === 'openfang:codex_oauth' && data.status === 'connected') {
          self.checkOAuthStatus().then(function() { return self.loadOnboardingStatus(); });
        }
      });
      await this.refreshAll();
    },

    async refreshAll() {
      this.loading = true;
      this.loadError = '';
      try {
        var tasks = [
          this.checkOAuthStatus(),
          this.loadProfile(),
          this.loadRuns(),
          this.loadProspects(),
          this.loadSourceHealth(),
          this.loadOnboardingStatus()
        ];
        if (!this.isB2C) {
          tasks.push(this.loadLeads());
          tasks.push(this.loadApprovals());
          tasks.push(this.loadDeliveries());
        } else {
          this.leads = [];
          this.approvals = [];
          this.deliveries = [];
          this.selectedDossier = null;
        }
        await Promise.all(tasks);
        await this.restoreJobProgress();
      } catch (e) {
        this.loadError = e && e.message ? e.message : 'Satis paneli yuklenemedi.';
      }
      this.syncApprovalCursor();
      if (!this.showOnboarding && !this.isB2C) {
        await this.loadSelectedDossier();
      }
      this.loading = false;
    },

    persistCurrentJobId(jobId) {
      try {
        if (jobId) {
          localStorage.setItem(this.currentJobStorageKey(), jobId);
        }
      } catch (e) {}
    },

    clearPersistedJobId() {
      try {
        localStorage.removeItem(this.currentJobStorageKey());
      } catch (e) {}
    },

    async restoreJobProgress() {
      var storedJobId = '';
      try {
        storedJobId = localStorage.getItem(this.currentJobStorageKey()) || '';
      } catch (e) {}

      try {
        var active = await OpenFangAPI.get(this.buildSalesUrl('/api/sales/jobs/active'));
        if (active && active.job && active.job.job_id) {
          this.currentJobId = active.job.job_id;
          this.jobProgress = active.job;
          this.persistCurrentJobId(active.job.job_id);
          if (active.job.status === 'running') {
            this.startJobPolling(active.job.job_id);
          }
          return;
        }
      } catch (e) {}

      if (!storedJobId) return;
      try {
        var progress = await OpenFangAPI.get('/api/sales/jobs/' + encodeURIComponent(storedJobId) + '/progress');
        if (!progress || !progress.job_id) {
          this.clearPersistedJobId();
          return;
        }
        this.currentJobId = progress.job_id;
        this.jobProgress = progress;
        if (progress.status === 'running') {
          this.startJobPolling(progress.job_id);
        }
      } catch (e) {
        this.clearPersistedJobId();
      }
    },

    async loadOnboardingStatus() {
      var data = await OpenFangAPI.get(this.buildSalesUrl('/api/sales/onboarding/status'));
      var s = (data && data.status) || {};
      this.onboarding = {
        completed: !!s.completed,
        active_step: Number(s.active_step || 1),
        steps: Array.isArray(s.steps) ? s.steps : [],
        oauth_connected: !!s.oauth_connected,
        has_brief: !!s.has_brief,
        profile_ready: !!s.profile_ready,
        first_run_ready: !!s.first_run_ready,
        brief: s.brief || '',
        last_successful_run_id: s.last_successful_run_id || ''
      };
      if (this.onboarding.brief && !this.profileBrief.trim()) {
        this.profileBrief = this.onboarding.brief;
      }
      if (this.onboarding.last_successful_run_id) {
        if (this.isB2C) {
          this.runLeads = [];
          await this.loadRunProspects(this.onboarding.last_successful_run_id);
        } else {
          await Promise.all([
            this.loadRunProspects(this.onboarding.last_successful_run_id),
            this.loadRunLeads(this.onboarding.last_successful_run_id)
          ]);
        }
      } else {
        this.selectedRunId = '';
        this.runProspects = [];
        this.runLeads = [];
      }
    },

    async loadProfile() {
      var data = await OpenFangAPI.get(this.buildSalesUrl('/api/sales/profile'));
      this.profile = this.normalizeProfile(data.profile || {});
    },

    async saveProfile() {
      this.savingProfile = true;
      try {
        var payload = this.normalizeProfile(this.profile);
        if (!payload.product_name.trim() || !payload.product_description.trim() || !payload.target_industry.trim() || !payload.target_geo.trim() || !payload.sender_name.trim() || !payload.sender_email.trim()) {
          throw new Error('Profil eksik: urun, aciklama, sektor, cografi hedef, gonderen adi ve e-posta zorunlu.');
        }
        if (payload.schedule_hour_local < 0 || payload.schedule_hour_local > 23) {
          throw new Error('Gunluk calisma saati 0-23 araliginda olmali.');
        }
        var data = await OpenFangAPI.put(this.buildSalesUrl('/api/sales/profile'), payload);
        if (data && data.profile) {
          this.profile = this.normalizeProfile(data.profile);
        }
        OpenFangToast.success('Profil kaydedildi');
        await this.loadOnboardingStatus();
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Profil kaydedilemedi');
      }
      this.savingProfile = false;
    },

    async saveBriefAndAutofill() {
      if (!this.profileBrief || this.profileBrief.trim().length < 20) {
        OpenFangToast.error('Brief cok kisa. En az 20 karakter girin.');
        return;
      }
      this.autofillingProfile = true;
      try {
        var data = await OpenFangAPI.post(this.buildSalesUrl('/api/sales/onboarding/brief'), {
          brief: this.profileBrief.trim(),
          persist: true
        });
        this.profile = this.normalizeProfile((data && data.profile) || {});
        if (data && data.onboarding) {
          this.onboarding = data.onboarding;
        } else {
          await this.loadOnboardingStatus();
        }
        var source = (data && data.source) || 'autofill';
        OpenFangToast.success('Brief cozuldu ve profil dolduruldu (' + source + ')');
        if (data && Array.isArray(data.warnings) && data.warnings.length) {
          OpenFangToast.warn(data.warnings[0], 8000);
        }
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Brief islenemedi');
      }
      this.autofillingProfile = false;
    },

    async runNow() {
      this.runningNow = true;
      try {
        var data = await OpenFangAPI.post(this.buildSalesUrl('/api/sales/run'), {});
        var jobId = (data && data.job_id) || '';
        if (!jobId) {
          throw new Error('Job baslatildi ancak job_id donmedi');
        }
        this.currentJobId = jobId;
        this.persistCurrentJobId(jobId);
        this.jobProgress = {
          job_id: jobId,
          status: (data && data.status) || 'running',
          current_stage: (data && data.current_stage) || 'QueryPlanning',
          stages: []
        };
        this.startJobPolling(jobId);
        OpenFangToast.success(
          this.isB2C
            ? 'Social discovery job baslatildi. Asamalar canli izleniyor.'
            : 'Prospecting job baslatildi. Asamalar canli izleniyor.'
        );
      } catch (e) {
        OpenFangToast.error(
          e && e.message
            ? e.message
            : (this.isB2C ? 'B2C social discovery basarisiz' : 'Aday musteri kesfi basarisiz')
        );
      } finally {
        this.runningNow = false;
      }
    },

    async loadRuns() {
      var data = await OpenFangAPI.get(this.buildSalesUrl('/api/sales/runs', { limit: 20 }));
      this.runs = data.runs || [];
    },

    async loadProspects() {
      var data = await OpenFangAPI.get(this.buildSalesUrl('/api/sales/prospects', { limit: 200 }));
      this.prospects = data.prospects || [];
      this.ensureProspectSelection();
      if (!this.isB2C) {
        await this.loadSelectedDossier();
      }
    },

    async loadLeads() {
      if (this.isB2C) {
        this.leads = [];
        return;
      }
      var data = await OpenFangAPI.get(this.buildSalesUrl('/api/sales/leads', { limit: 200 }));
      this.leads = data.leads || [];
    },

    async loadRunProspects(runId) {
      this.selectedRunId = runId || '';
      if (!runId) {
        this.runProspects = [];
        this.ensureProspectSelection();
        return;
      }
      var data = await OpenFangAPI.get(this.buildSalesUrl('/api/sales/prospects', { limit: 100, run_id: runId }));
      this.runProspects = data.prospects || [];
      this.ensureProspectSelection();
      if (!this.isB2C) {
        await this.loadSelectedDossier();
      }
    },

    async loadRunLeads(runId) {
      if (this.isB2C) {
        this.runLeads = [];
        return;
      }
      this.selectedRunId = runId || '';
      if (!runId) {
        this.runLeads = [];
        return;
      }
      var data = await OpenFangAPI.get(this.buildSalesUrl('/api/sales/leads', { limit: 100, run_id: runId }));
      this.runLeads = data.leads || [];
    },

    async loadApprovals() {
      var data = await OpenFangAPI.get('/api/sales/approvals?limit=200');
      this.approvals = data.approvals || [];
      this.pruneApprovalState();
      this.syncApprovalCursor();
    },

    async loadDeliveries() {
      var data = await OpenFangAPI.get('/api/sales/deliveries?limit=200');
      this.deliveries = data.deliveries || [];
    },

    async loadSourceHealth() {
      var data = await OpenFangAPI.get('/api/sales/source-health');
      this.sourceHealth = data.sources || [];
    },

    tabHash(tab) {
      return '#' + this.routePage() + '/' + tab;
    },

    syncTabFromHash() {
      var raw = (window.location.hash || '').replace('#', '').trim().toLowerCase();
      var key = raw.split('/')[1] || 'command';
      if (['command', 'profiles', 'approvals', 'deliveries'].indexOf(key) >= 0) {
        if (this.isB2C && (key === 'approvals' || key === 'deliveries')) {
          this.activeTab = 'profiles';
          return;
        }
        this.activeTab = key;
      }
    },

    setTab(tab) {
      if (['command', 'profiles', 'approvals', 'deliveries'].indexOf(tab) === -1) return;
      if (this.isB2C && (tab === 'approvals' || tab === 'deliveries')) return;
      this.activeTab = tab;
      if (window.location.hash !== this.tabHash(tab)) {
        history.replaceState(null, '', this.tabHash(tab));
      }
    },

    async loadSelectedDossier() {
      if (this.isB2C) {
        this.selectedDossier = null;
        return;
      }
      var prospect = this.selectedProspectRecord();
      if (!prospect || !prospect.company_domain) {
        this.selectedDossier = null;
        return;
      }
      this.dossierLoading = true;
      try {
        var data = await OpenFangAPI.get('/api/sales/accounts/' + encodeURIComponent(prospect.company_domain) + '/dossier');
        this.selectedDossier = data && data.dossier ? data.dossier : null;
      } catch (_) {
        this.selectedDossier = null;
      }
      this.dossierLoading = false;
    },
};
