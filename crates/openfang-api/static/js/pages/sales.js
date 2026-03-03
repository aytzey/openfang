'use strict';

function salesPage() {
  return {
    loading: true,
    loadError: '',
    savingProfile: false,
    autofillingProfile: false,
    runningNow: false,
    oauthBusy: false,

    profileBrief: '',
    profile: {
      product_name: '',
      product_description: '',
      target_industry: '',
      target_geo: 'TR',
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
    runs: [],
    leads: [],
    runLeads: [],
    approvals: [],
    deliveries: [],

    get showOnboarding() {
      return !this.onboarding.completed;
    },

    get pendingApprovals() {
      return this.approvals.filter(function(a) { return a.status === 'pending'; }).length;
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
        target_geo: src.target_geo || 'TR',
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
          this.loadLeads(),
          this.loadApprovals(),
          this.loadDeliveries(),
          this.loadOnboardingStatus()
        ]);
      } catch (e) {
        this.loadError = e && e.message ? e.message : 'Satis paneli yuklenemedi.';
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
        await this.loadRunLeads(this.onboarding.last_successful_run_id);
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
        if (!payload.product_name.trim() || !payload.product_description.trim() || !payload.target_industry.trim() || !payload.sender_name.trim() || !payload.sender_email.trim()) {
          throw new Error('Profil eksik: urun, aciklama, sektor, gonderen adi ve e-posta zorunlu.');
        }
        if (payload.schedule_hour_local < 0 || payload.schedule_hour_local > 23) {
          throw new Error('Gunluk calisma saati 0-23 araliginda olmali.');
        }
        await OpenFangAPI.put('/api/sales/profile', payload);
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
        var run = (data && data.run) || {};
        OpenFangToast.success('Lead uretimi tamamlandi. Eklenen: ' + String(run.inserted || 0));
        if (run.id) {
          await this.loadRunLeads(run.id);
        }
        await Promise.all([
          this.loadRuns(),
          this.loadLeads(),
          this.loadApprovals(),
          this.loadDeliveries(),
          this.loadOnboardingStatus()
        ]);
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Lead uretimi basarisiz');
      }
      this.runningNow = false;
    },

    async loadRuns() {
      var data = await OpenFangAPI.get('/api/sales/runs?limit=20');
      this.runs = data.runs || [];
    },

    async loadLeads() {
      var data = await OpenFangAPI.get('/api/sales/leads?limit=200');
      this.leads = data.leads || [];
    },

    async loadRunLeads(runId) {
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
    },

    async loadDeliveries() {
      var data = await OpenFangAPI.get('/api/sales/deliveries?limit=200');
      this.deliveries = data.deliveries || [];
    },

    approvalRecipient(a) {
      if (!a || !a.payload) return '-';
      if (a.channel === 'email') return a.payload.to || '-';
      if (a.channel === 'linkedin') return a.payload.profile_url || '-';
      return '-';
    },

    async approve(id) {
      try {
        await OpenFangAPI.post('/api/sales/approvals/' + encodeURIComponent(id) + '/approve', {});
        OpenFangToast.success('Onaylandi ve gonderildi');
        await Promise.all([this.loadApprovals(), this.loadDeliveries()]);
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Onay islemi basarisiz');
      }
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
        this.oauth.connected = false;
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
