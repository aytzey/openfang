'use strict';

function salesPage() {
  return {
    loading: true,
    loadError: '',
    savingProfile: false,
    autofillingProfile: false,
    runningNow: false,
    oauthBusy: false,
    currentJobId: '',
    jobPollTimer: null,
    jobProgress: null,
    sourceHealth: [],
    activeTab: 'command',
    selectedDossier: null,
    dossierLoading: false,
    approvalSelections: {},
    approvalDrafts: {},
    approvalEditorId: '',
    approvalCursorId: '',

    profileBrief: '',
    profile: {
      product_name: '',
      product_description: '',
      target_industry: '',
      target_geo: '',
      sender_name: '',
      sender_email: '',
      sender_linkedin: '',
      target_title_policy: 'ceo_then_founder',
      daily_target: 20,
      daily_send_cap: 20,
      schedule_hour_local: 9,
      timezone_mode: 'local'
    },

    onboarding: {
      completed: false,
      active_step: 1,
      steps: [],
      oauth_connected: false,
      has_brief: false,
      profile_ready: false,
      first_run_ready: false,
      brief: '',
      last_successful_run_id: ''
    },

    oauth: {
      connected: false,
      source: '',
      reason: '',
      issued_at: null,
      expires_at: null,
      has_refresh_token: false,
      auth_url: '',
      state: ''
    },

    manualCode: '',
    selectedRunId: '',
    selectedProspectId: '',
    runs: [],
    prospects: [],
    runProspects: [],
    leads: [],
    runLeads: [],
    approvals: [],
    deliveries: [],

    get showOnboarding() {
      return !this.onboarding.completed;
    },

    get tabItems() {
      return [
        { key: 'command', label: 'Command Center' },
        { key: 'profiles', label: 'Profiles' },
        { key: 'approvals', label: 'Approval Queue' },
        { key: 'deliveries', label: 'Delivery' }
      ];
    },

    get pendingApprovals() {
      return this.approvals.filter(function(a) { return a.status === 'pending'; }).length;
    },

    get pendingApprovalItems() {
      return this.approvals.filter(function(a) { return a.status === 'pending'; });
    },

    get contactReadyProspects() {
      return this.prospects.filter(function(p) { return p.profile_status === 'contact_ready'; }).length;
    },

    get companyOnlyProspects() {
      return this.prospects.filter(function(p) { return p.profile_status === 'company_only'; }).length;
    },

    get selectedApprovalCount() {
      var ids = Object.keys(this.approvalSelections || {});
      var count = 0;
      for (var i = 0; i < ids.length; i++) {
        if (this.approvalSelections[ids[i]]) count += 1;
      }
      return count;
    },

    get selectedDossierScore() {
      return this.selectedDossier && this.selectedDossier.score ? this.selectedDossier.score : null;
    },

    get selectedDossierOutcomes() {
      return this.selectedDossier && this.selectedDossier.outcomes ? this.selectedDossier.outcomes : null;
    },

    get selectedPositiveReplyRate() {
      var outcomes = this.selectedDossierOutcomes;
      return outcomes ? this.asPercent(outcomes.positive_reply_rate) : '-';
    },

    get selectedMeetingRate() {
      var outcomes = this.selectedDossierOutcomes;
      return outcomes ? this.asPercent(outcomes.meeting_rate) : '-';
    },

    get selectedAccountLabel() {
      var dossier = this.selectedDossier;
      if (dossier && dossier.account && dossier.account.display_name) return dossier.account.display_name;
      var prospect = this.selectedProspectRecord();
      return prospect ? prospect.company : 'Secili account';
    },

    selectedProspectRecord() {
      var pools = [this.runProspects, this.prospects];
      for (var i = 0; i < pools.length; i++) {
        var pool = pools[i] || [];
        for (var j = 0; j < pool.length; j++) {
          if (pool[j].id === this.selectedProspectId) return pool[j];
        }
      }
      if (this.runProspects.length > 0) return this.runProspects[0];
      if (this.prospects.length > 0) return this.prospects[0];
      return null;
    },

    stepDone(key) {
      var steps = this.onboarding.steps || [];
      for (var i = 0; i < steps.length; i++) {
        if (steps[i].key === key) return !!steps[i].done;
      }
      return false;
    },

    normalizeProfile(p) {
      var src = p || {};
      return {
        product_name: src.product_name || '',
        product_description: src.product_description || '',
        target_industry: src.target_industry || '',
        target_geo: src.target_geo || '',
        sender_name: src.sender_name || '',
        sender_email: src.sender_email || '',
        sender_linkedin: src.sender_linkedin || '',
        target_title_policy: src.target_title_policy || 'ceo_then_founder',
        daily_target: Number(src.daily_target || 20),
        daily_send_cap: Number(src.daily_send_cap || 20),
        schedule_hour_local: Number(src.schedule_hour_local || 9),
        timezone_mode: src.timezone_mode || 'local'
      };
    },

    async init() {
      var self = this;
      this.syncTabFromHash();
      window.addEventListener('beforeunload', function() {
        self.stopJobPolling();
      });
      window.addEventListener('hashchange', function() {
        self.syncTabFromHash();
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
        await Promise.all([
          this.checkOAuthStatus(),
          this.loadProfile(),
          this.loadRuns(),
          this.loadProspects(),
          this.loadLeads(),
          this.loadApprovals(),
          this.loadDeliveries(),
          this.loadSourceHealth(),
          this.loadOnboardingStatus()
        ]);
      } catch (e) {
        this.loadError = e && e.message ? e.message : 'Satis paneli yuklenemedi.';
      }
      this.syncApprovalCursor();
      if (!this.showOnboarding) {
        await this.loadSelectedDossier();
      }
      this.loading = false;
    },

    async loadOnboardingStatus() {
      var data = await OpenFangAPI.get('/api/sales/onboarding/status');
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
        await Promise.all([
          this.loadRunProspects(this.onboarding.last_successful_run_id),
          this.loadRunLeads(this.onboarding.last_successful_run_id)
        ]);
      } else {
        this.selectedRunId = '';
        this.runProspects = [];
        this.runLeads = [];
      }
    },

    async loadProfile() {
      var data = await OpenFangAPI.get('/api/sales/profile');
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
        var data = await OpenFangAPI.put('/api/sales/profile', payload);
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
        var data = await OpenFangAPI.post('/api/sales/onboarding/brief', {
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
        var data = await OpenFangAPI.post('/api/sales/run', {});
        var jobId = (data && data.job_id) || '';
        if (!jobId) {
          throw new Error('Job baslatildi ancak job_id donmedi');
        }
        this.currentJobId = jobId;
        this.jobProgress = {
          job_id: jobId,
          status: (data && data.status) || 'running',
          current_stage: (data && data.current_stage) || 'QueryPlanning',
          stages: []
        };
        this.startJobPolling(jobId);
        OpenFangToast.success('Prospecting job baslatildi. Asamalar canli izleniyor.');
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Aday musteri kesfi basarisiz');
      } finally {
        this.runningNow = false;
      }
    },

    async loadRuns() {
      var data = await OpenFangAPI.get('/api/sales/runs?limit=20');
      this.runs = data.runs || [];
    },

    async loadProspects() {
      var data = await OpenFangAPI.get('/api/sales/prospects?limit=200');
      this.prospects = data.prospects || [];
      this.ensureProspectSelection();
      await this.loadSelectedDossier();
    },

    async loadLeads() {
      var data = await OpenFangAPI.get('/api/sales/leads?limit=200');
      this.leads = data.leads || [];
    },

    async loadRunProspects(runId) {
      this.selectedRunId = runId || '';
      if (!runId) {
        this.runProspects = [];
        this.ensureProspectSelection();
        return;
      }
      var data = await OpenFangAPI.get('/api/sales/prospects?limit=100&run_id=' + encodeURIComponent(runId));
      this.runProspects = data.prospects || [];
      this.ensureProspectSelection();
      await this.loadSelectedDossier();
    },

    async loadRunLeads(runId) {
      this.selectedRunId = runId || '';
      if (!runId) {
        this.runLeads = [];
        return;
      }
      var data = await OpenFangAPI.get('/api/sales/leads?limit=100&run_id=' + encodeURIComponent(runId));
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
      return '#sales-' + tab;
    },

    syncTabFromHash() {
      var raw = window.location.hash || '';
      if (raw.indexOf('#sales-') !== 0) return;
      var key = raw.slice(7);
      if (['command', 'profiles', 'approvals', 'deliveries'].indexOf(key) >= 0) {
        this.activeTab = key;
      }
    },

    setTab(tab) {
      if (['command', 'profiles', 'approvals', 'deliveries'].indexOf(tab) === -1) return;
      this.activeTab = tab;
      if (window.location.hash !== this.tabHash(tab)) {
        history.replaceState(null, '', this.tabHash(tab));
      }
    },

    async loadSelectedDossier() {
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

    selectedScoreBars() {
      var score = this.selectedDossierScore;
      if (!score) return [];
      return [
        { key: 'fit', label: 'Fit', value: Number(score.fit_score || 0), risk: false },
        { key: 'intent', label: 'Intent', value: Number(score.intent_score || 0), risk: false },
        { key: 'reach', label: 'Reach', value: Number(score.reachability_score || 0), risk: false },
        { key: 'deliverability', label: 'Deliverability Risk', value: Number(score.deliverability_risk || 0), risk: true },
        { key: 'compliance', label: 'Compliance Risk', value: Number(score.compliance_risk || 0), risk: true }
      ];
    },

    scoreBarWidth(bar) {
      return String(Math.round(Math.max(0, Math.min(1, Number(bar && bar.value ? bar.value : 0))) * 100)) + '%';
    },

    scoreBarClass(bar) {
      var value = Number(bar && bar.value ? bar.value : 0);
      if (bar && bar.risk) {
        return value > 0.5 ? 'is-bad' : (value > 0.25 ? 'is-warn' : 'is-good');
      }
      return value >= 0.65 ? 'is-good' : (value >= 0.35 ? 'is-warn' : 'is-bad');
    },

    sendGateLabel() {
      var gate = this.selectedDossierScore && this.selectedDossierScore.send_gate;
      if (!gate || !gate.decision) return '-';
      return gate.decision.replace(/_/g, ' ');
    },

    sendGateReason() {
      var gate = this.selectedDossierScore && this.selectedDossierScore.send_gate;
      if (!gate || !gate.decision) return '';
      if (gate.reason) return gate.reason;
      if (Array.isArray(gate.missing)) return gate.missing.join('; ');
      return '';
    },

    asPercent(value) {
      if (typeof value !== 'number' || isNaN(value)) return '-';
      return String(Math.round(value * 100)) + '%';
    },

    approvalSelected(id) {
      return !!(this.approvalSelections && this.approvalSelections[id]);
    },

    toggleApprovalSelection(id, checked) {
      if (!id) return;
      this.approvalSelections[id] = !!checked;
    },

    toggleAllPendingApprovals(checked) {
      var items = this.pendingApprovalItems;
      for (var i = 0; i < items.length; i++) {
        this.approvalSelections[items[i].id] = !!checked;
      }
    },

    pruneApprovalState() {
      var keep = {};
      var ids = {};
      for (var i = 0; i < this.pendingApprovalItems.length; i++) {
        ids[this.pendingApprovalItems[i].id] = true;
      }
      var selectionIds = Object.keys(this.approvalSelections || {});
      for (var j = 0; j < selectionIds.length; j++) {
        if (ids[selectionIds[j]]) keep[selectionIds[j]] = !!this.approvalSelections[selectionIds[j]];
      }
      this.approvalSelections = keep;

      var draftKeep = {};
      var draftIds = Object.keys(this.approvalDrafts || {});
      for (var k = 0; k < draftIds.length; k++) {
        if (ids[draftIds[k]]) draftKeep[draftIds[k]] = this.approvalDrafts[draftIds[k]];
      }
      this.approvalDrafts = draftKeep;

      if (this.approvalEditorId && !ids[this.approvalEditorId]) {
        this.approvalEditorId = '';
      }
    },

    syncApprovalCursor() {
      var items = this.pendingApprovalItems;
      if (!items.length) {
        this.approvalCursorId = '';
        return;
      }
      for (var i = 0; i < items.length; i++) {
        if (items[i].id === this.approvalCursorId) return;
      }
      this.approvalCursorId = items[0].id;
    },

    moveApprovalCursor(delta) {
      var items = this.pendingApprovalItems;
      if (!items.length) {
        this.approvalCursorId = '';
        return;
      }
      var idx = 0;
      for (var i = 0; i < items.length; i++) {
        if (items[i].id === this.approvalCursorId) {
          idx = i;
          break;
        }
      }
      idx = (idx + delta + items.length) % items.length;
      this.approvalCursorId = items[idx].id;
    },

    currentPendingApproval() {
      var items = this.pendingApprovalItems;
      for (var i = 0; i < items.length; i++) {
        if (items[i].id === this.approvalCursorId) return items[i];
      }
      return items.length ? items[0] : null;
    },

    approvalRowClass(approval) {
      return approval && approval.id === this.approvalCursorId ? 'is-active' : '';
    },

    approvalClassification(approval) {
      return approval && approval.payload && approval.payload.classification
        ? approval.payload.classification
        : '';
    },

    approvalHasWarning(approval) {
      var cls = this.approvalClassification(approval);
      return cls === 'generic' || cls === 'role' || cls === 'consumer';
    },

    approvalWarningLabel(approval) {
      var cls = this.approvalClassification(approval);
      if (cls === 'generic' || cls === 'role') return 'Generic email - low reply probability';
      if (cls === 'consumer') return 'Consumer mailbox - blocked by policy';
      return '';
    },

    approvalDraft(approval) {
      if (!approval || !approval.id) return {};
      if (!this.approvalDrafts[approval.id]) {
        var payload = approval.payload || {};
        this.approvalDrafts[approval.id] = {
          to: payload.to || '',
          profile_url: payload.profile_url || '',
          subject: payload.subject || '',
          body: payload.body || '',
          message: payload.message || '',
          icebreaker: this.extractIcebreaker(approval)
        };
      }
      return this.approvalDrafts[approval.id];
    },

    extractIcebreaker(approval) {
      if (!approval || !approval.payload) return '';
      var text = approval.channel === 'email' ? (approval.payload.body || '') : (approval.payload.message || '');
      if (!text) return '';
      var parts = text.split(/\n+/);
      return parts[0] || '';
    },

    approvalBodyRemainder(approval) {
      if (!approval || !approval.payload) return '';
      var text = approval.channel === 'email' ? (approval.payload.body || '') : (approval.payload.message || '');
      if (!text) return '';
      var parts = text.split(/\n+/);
      parts.shift();
      return parts.join('\n').trim();
    },

    startApprovalEdit(approval) {
      if (!approval || !approval.id) return;
      this.approvalEditorId = approval.id;
      this.approvalCursorId = approval.id;
      this.approvalDraft(approval);
    },

    cancelApprovalEdit() {
      this.approvalEditorId = '';
    },

    buildEditedPayload(approval) {
      var draft = this.approvalDraft(approval);
      if (approval.channel === 'email') {
        return {
          to: draft.to || (approval.payload && approval.payload.to) || '',
          subject: draft.subject || '',
          body: draft.body || ''
        };
      }
      return {
        profile_url: draft.profile_url || (approval.payload && approval.payload.profile_url) || '',
        message: draft.message || ''
      };
    },

    async saveApprovalEdit(id) {
      var approval = null;
      for (var i = 0; i < this.pendingApprovalItems.length; i++) {
        if (this.pendingApprovalItems[i].id === id) {
          approval = this.pendingApprovalItems[i];
          break;
        }
      }
      if (!approval) {
        OpenFangToast.error('Duzenlenecek approval bulunamadi');
        return;
      }
      try {
        var data = await OpenFangAPI.patch(
          '/api/sales/approvals/' + encodeURIComponent(id) + '/edit',
          { edited_payload: this.buildEditedPayload(approval) }
        );
        var updated = data && data.approval ? data.approval : null;
        if (updated) {
          for (var j = 0; j < this.approvals.length; j++) {
            if (this.approvals[j].id === id) {
              this.approvals[j] = updated;
              break;
            }
          }
          delete this.approvalDrafts[id];
        }
        this.approvalEditorId = '';
        OpenFangToast.success('Taslak guncellendi');
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Taslak guncellenemedi');
      }
    },

    async bulkApproveSelected() {
      var ids = [];
      var items = this.pendingApprovalItems;
      for (var i = 0; i < items.length; i++) {
        if (this.approvalSelections[items[i].id]) ids.push(items[i].id);
      }
      if (!ids.length) {
        OpenFangToast.error('Onaylanacak taslak secilmedi');
        return;
      }
      try {
        var data = await OpenFangAPI.post('/api/sales/approvals/bulk-approve', { ids: ids });
        var approved = data && Array.isArray(data.approved) ? data.approved.length : 0;
        var failed = data && Array.isArray(data.failed) ? data.failed.length : 0;
        this.approvalSelections = {};
        await Promise.all([this.loadApprovals(), this.loadDeliveries()]);
        if (approved > 0) {
          OpenFangToast.success(String(approved) + ' taslak onaylandi');
        }
        if (failed > 0) {
          OpenFangToast.warn(String(failed) + ' taslak gonderilemedi', 8000);
        }
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Toplu onay basarisiz');
      }
    },

    handleApprovalHotkeys(evt) {
      if (this.showOnboarding || this.activeTab !== 'approvals') return;
      var tag = document.activeElement && document.activeElement.tagName ? document.activeElement.tagName.toUpperCase() : '';
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
      if (!this.pendingApprovalItems.length) return;

      if (evt.key === 'ArrowDown') {
        evt.preventDefault();
        this.moveApprovalCursor(1);
        return;
      }
      if (evt.key === 'ArrowRight') {
        evt.preventDefault();
        var current = this.currentPendingApproval();
        if (current) this.approve(current.id);
        return;
      }
      if (evt.key === 'ArrowLeft') {
        evt.preventDefault();
        var currentReject = this.currentPendingApproval();
        if (currentReject) this.reject(currentReject.id);
        return;
      }
      if (evt.key === 'ArrowUp') {
        evt.preventDefault();
        var currentEdit = this.currentPendingApproval();
        if (currentEdit) this.startApprovalEdit(currentEdit);
      }
    },

    stopJobPolling() {
      if (this.jobPollTimer) {
        clearTimeout(this.jobPollTimer);
        this.jobPollTimer = null;
      }
    },

    startJobPolling(jobId) {
      this.stopJobPolling();
      if (!jobId) return;
      var self = this;
      var tick = async function() {
        try {
          var data = await OpenFangAPI.get('/api/sales/jobs/' + encodeURIComponent(jobId) + '/progress');
          self.jobProgress = data || null;
          if (!data || data.status === 'completed' || data.status === 'failed') {
            self.stopJobPolling();
            self.currentJobId = jobId;
            await Promise.all([
              self.loadRuns(),
              self.loadProspects(),
              self.loadLeads(),
              self.loadApprovals(),
              self.loadDeliveries(),
              self.loadSourceHealth(),
              self.loadOnboardingStatus()
            ]);
            var latestRun = self.runs.length ? self.runs[0] : null;
            if (latestRun && latestRun.id) {
              await Promise.all([
                self.loadRunProspects(latestRun.id),
                self.loadRunLeads(latestRun.id)
              ]);
            }
            if (data && data.status === 'completed') {
              OpenFangToast.success('Prospecting job tamamlandi');
            } else if (data && data.error_message) {
              OpenFangToast.error(data.error_message);
            } else {
              OpenFangToast.error('Prospecting job basarisiz');
            }
            return;
          }
        } catch (e) {
          self.stopJobPolling();
          OpenFangToast.error(e && e.message ? e.message : 'Job ilerlemesi alinamadi');
          return;
        }
        self.jobPollTimer = setTimeout(tick, 2500);
      };
      tick();
    },

    approvalRecipient(a) {
      if (!a || !a.payload) return '-';
      if (a.channel === 'email') return a.payload.to || '-';
      if (a.channel === 'linkedin' || a.channel === 'linkedin_assist') return a.payload.profile_url || '-';
      return '-';
    },

    approvalTitle(a) {
      if (!a || !a.payload) return 'Taslak';
      if (a.channel === 'email') return a.payload.subject || 'E-posta taslagi';
      if (a.channel === 'linkedin' || a.channel === 'linkedin_assist') return 'LinkedIn operator assist';
      return 'Taslak';
    },

    approvalBody(a) {
      if (!a || !a.payload) return '';
      if (a.channel === 'email') return a.payload.body || '';
      if (a.channel === 'linkedin' || a.channel === 'linkedin_assist') return a.payload.message || '';
      return '';
    },

    leadReasonsPreview(lead) {
      if (!lead || !Array.isArray(lead.reasons)) return [];
      return lead.reasons.slice(0, 2);
    },

    prospectSignalsPreview(prospect) {
      if (!prospect || !Array.isArray(prospect.matched_signals)) return [];
      return prospect.matched_signals.slice(0, 3);
    },

    prospectPrimaryContact(prospect) {
      if (!prospect) return '-';
      var name = prospect.primary_contact_name || 'Temas yok';
      var title = prospect.primary_contact_title || '';
      return title ? (name + ' / ' + title) : name;
    },

    prospectChannels(prospect) {
      if (!prospect) return '-';
      var channels = [];
      if (prospect.primary_email) channels.push('email');
      if (prospect.primary_linkedin_url) channels.push('linkedin_assist');
      return channels.length ? channels.join(' + ') : 'sirket seviyesi';
    },

    prospectNextAction(prospect) {
      if (!prospect) return '-';
      if (prospect.profile_status === 'contact_ready') {
        if (prospect.primary_email && prospect.primary_linkedin_url) return 'Email ile basla, LinkedIn follow-up';
        if (prospect.primary_email) return 'Email taslagini onaya gonder';
        if (prospect.primary_linkedin_url) return 'LinkedIn operator-assist gorevi ac';
      }
      if (prospect.profile_status === 'contact_identified') return "Kanal dogrulama yap ve lead'e yuksel";
      return 'Buying committee ve temas kanali cikar';
    },

    prospectOsintLinks(prospect) {
      if (!prospect) return [];
      var links = [];
      if (prospect.primary_linkedin_url) links.push(prospect.primary_linkedin_url);
      if (prospect.company_linkedin_url) links.push(prospect.company_linkedin_url);
      if (Array.isArray(prospect.osint_links)) {
        links = links.concat(prospect.osint_links);
      }
      var seen = new Set();
      return links
        .filter(function(link) { return typeof link === 'string' && link.trim().length > 0; })
        .map(function(link) { return link.trim(); })
        .filter(function(link) {
          if (seen.has(link)) return false;
          seen.add(link);
          return true;
        })
        .slice(0, 6);
    },

    prospectStatusClass(status) {
      if (status === 'contact_ready') return 'badge-success';
      if (status === 'contact_identified') return 'badge-warn';
      return 'badge-muted';
    },

    prospectResearchClass(status) {
      return status === 'llm_enriched' ? 'badge-success' : 'badge-muted';
    },

    prospectConfidencePct(prospect) {
      if (!prospect || typeof prospect.research_confidence !== 'number') return '0%';
      return String(Math.round(prospect.research_confidence * 100)) + '%';
    },

    async selectProspect(prospect) {
      if (!prospect || !prospect.id) return;
      this.selectedProspectId = prospect.id;
      await this.loadSelectedDossier();
    },

    ensureProspectSelection() {
      var current = this.selectedProspectRecord();
      if (current && current.id) {
        this.selectedProspectId = current.id;
        return;
      }
      if (this.runProspects.length > 0) {
        this.selectedProspectId = this.runProspects[0].id;
        return;
      }
      if (this.prospects.length > 0) {
        this.selectedProspectId = this.prospects[0].id;
        return;
      }
      this.selectedProspectId = '';
    },

    runBadgeClass(run) {
      if (!run) return 'badge-muted';
      if (run.status === 'completed') return 'badge-success';
      if (run.status === 'running') return 'badge-warn';
      if (run.status === 'failed') return 'badge-error';
      return 'badge-muted';
    },

    jobStageBadgeClass(stage) {
      if (!stage) return 'badge-muted';
      if (stage.status === 'completed') return 'badge-success';
      if (stage.status === 'running') return 'badge-warn';
      if (stage.status === 'failed') return 'badge-error';
      return 'badge-muted';
    },

    sourceHealthBadgeClass(row) {
      if (!row) return 'badge-muted';
      if (row.auto_skip || row.parser_health === 0) return 'badge-error';
      if (row.parser_health < 1) return 'badge-warn';
      return 'badge-success';
    },

    deliveryStatusClass(delivery) {
      if (!delivery) return 'badge-muted';
      if (delivery.status === 'sent') return 'badge-success';
      if (delivery.status === 'operator_pending') return 'badge-warn';
      if (delivery.status && delivery.status.indexOf('blocked') === 0) return 'badge-error';
      if (delivery.status === 'failed') return 'badge-error';
      return 'badge-muted';
    },

    async approve(id) {
      try {
        await OpenFangAPI.post('/api/sales/approvals/' + encodeURIComponent(id) + '/approve', {});
        OpenFangToast.success('Onaylandi');
        await Promise.all([this.loadApprovals(), this.loadDeliveries()]);
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Onay islemi basarisiz');
      }
    },

    async retryJob(forceFresh) {
      if (!this.currentJobId) {
        OpenFangToast.error('Tekrar denenecek job bulunamadi');
        return;
      }
      this.runningNow = true;
      try {
        var data = await OpenFangAPI.post(
          '/api/sales/jobs/' + encodeURIComponent(this.currentJobId) + '/retry',
          { force_fresh: !!forceFresh }
        );
        var jobId = (data && data.job_id) || '';
        if (!jobId) throw new Error('Retry job_id donmedi');
        this.currentJobId = jobId;
        this.jobProgress = {
          job_id: jobId,
          status: (data && data.status) || 'running',
          current_stage: 'QueryPlanning',
          stages: []
        };
        this.startJobPolling(jobId);
        OpenFangToast.success('Job retry baslatildi');
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Retry baslatilamadi');
      }
      this.runningNow = false;
    },

    async reject(id) {
      var self = this;
      OpenFangToast.confirm('Taslagi Reddet', 'Bu mesaj taslagi reddedilsin mi?', async function() {
        try {
          await OpenFangAPI.post('/api/sales/approvals/' + encodeURIComponent(id) + '/reject', { reason: 'manual_reject' });
          OpenFangToast.success('Reddedildi');
          await self.loadApprovals();
        } catch (e) {
          OpenFangToast.error(e && e.message ? e.message : 'Reddetme basarisiz');
        }
      });
    },

    async checkOAuthStatus() {
      this.oauthBusy = true;
      try {
        var status = await OpenFangAPI.get('/api/auth/codex/status');
        this.oauth = {
          connected: !!status.connected,
          source: status.source || '',
          reason: status.reason || '',
          issued_at: status.issued_at || null,
          expires_at: status.expires_at || null,
          has_refresh_token: !!status.has_refresh_token,
          auth_url: this.oauth.auth_url || '',
          state: this.oauth.state || ''
        };
      } catch (_) {
        this.oauth = {
          connected: false,
          source: '',
          reason: '',
          issued_at: null,
          expires_at: null,
          has_refresh_token: false,
          auth_url: '',
          state: ''
        };
      }
      this.oauthBusy = false;
    },

    async startOAuth() {
      this.oauthBusy = true;
      var popup = null;
      try {
        popup = window.open('', '_blank', 'width=560,height=760');
        if (popup && popup.document) {
          popup.document.title = 'OAuth Girisi';
          popup.document.body.innerHTML = '<div style="font-family:Arial,sans-serif;padding:24px;line-height:1.5">OAuth aciliyor...</div>';
        }
      } catch (_) {
        popup = null;
      }
      try {
        var res = await OpenFangAPI.post('/api/auth/codex/start', {});
        this.oauth.auth_url = res.auth_url || '';
        this.oauth.state = res.state || '';
        if (!this.oauth.auth_url) throw new Error('OAuth baslatilamadi: auth_url donmedi');

        if (popup && !popup.closed) {
          popup.location.replace(this.oauth.auth_url);
          popup.focus();
        } else {
          OpenFangToast.warn('Popup engellendi. Alttaki "OAuth Girisini Ac" baglantisini kullanin.');
        }

        await this.pollOAuthUntilConnected();
        await this.loadOnboardingStatus();
      } catch (e) {
        if (popup && !popup.closed) {
          try { popup.close(); } catch (_) {}
        }
        OpenFangToast.error(e && e.message ? e.message : 'OAuth baslatilamadi');
      }
      this.oauthBusy = false;
    },

    async submitManualCode() {
      if (!this.manualCode || !this.manualCode.trim()) {
        OpenFangToast.error('Lutfen OAuth kodunu yapistirin');
        return;
      }
      this.oauthBusy = true;
      try {
        await OpenFangAPI.post('/api/auth/codex/paste-code', {
          code: this.manualCode.trim(),
          state: this.oauth.state || undefined
        });
        this.manualCode = '';
        await this.checkOAuthStatus();
        await this.loadOnboardingStatus();
        OpenFangToast.success('Codex OAuth baglandi');
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Kod degisimi basarisiz');
      }
      this.oauthBusy = false;
    },

    async pollOAuthUntilConnected() {
      for (var i = 0; i < 24; i++) {
        await new Promise(function(resolve) { setTimeout(resolve, 2500); });
        await this.checkOAuthStatus();
        if (this.oauth.connected) {
          this.oauth.auth_url = '';
          OpenFangToast.success('Codex OAuth baglandi');
          return;
        }
      }
      OpenFangToast.warn('OAuth henuz dogrulanmadi. Giris tamamlandiysa "Durum Kontrol" butonuna basin.');
    },

    async importCliAuth() {
      this.oauthBusy = true;
      try {
        await OpenFangAPI.post('/api/auth/codex/import-cli', {});
        await this.checkOAuthStatus();
        await this.loadOnboardingStatus();
        OpenFangToast.success('~/.codex/auth.json ice aktarildi');
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Ice aktarma basarisiz');
      }
      this.oauthBusy = false;
    },

    async logoutOAuth() {
      this.oauthBusy = true;
      try {
        await OpenFangAPI.post('/api/auth/codex/logout', {});
        this.oauth = {
          connected: false,
          source: '',
          reason: '',
          issued_at: null,
          expires_at: null,
          has_refresh_token: false,
          auth_url: '',
          state: ''
        };
        this.manualCode = '';
        await this.loadOnboardingStatus();
        OpenFangToast.success('Codex OAuth baglantisi kesildi');
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Cikis islemi basarisiz');
      }
      this.oauthBusy = false;
    },

    formatDateTime(value) {
      if (!value) return '-';
      try {
        return new Date(value).toLocaleString('tr-TR');
      } catch (_) {
        return value;
      }
    }
  };
}
