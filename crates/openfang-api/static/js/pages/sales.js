// OpenFang Sales Page - lead generation + outreach approvals + Codex OAuth
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
      target_geo: 'US',
      sender_name: '',
      sender_email: '',
      sender_linkedin: '',
      target_title_policy: 'ceo_then_founder',
      daily_target: 20,
      daily_send_cap: 20,
      schedule_hour_local: 9,
      timezone_mode: 'local'
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
    approvals: [],
    deliveries: [],

    get pendingApprovals() {
      return this.approvals.filter(function(a) { return a.status === 'pending'; }).length;
    },

    normalizeProfile(p) {
      var src = p || {};
      return {
        product_name: src.product_name || '',
        product_description: src.product_description || '',
        target_industry: src.target_industry || '',
        target_geo: src.target_geo || 'US',
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
          this.loadDeliveries()
        ]);
      } catch(e) {
        this.loadError = e.message || 'Could not load sales page.';
      }
      this.loading = false;
    },

    async loadProfile() {
      var data = await OpenFangAPI.get('/api/sales/profile');
      this.profile = this.normalizeProfile(data.profile || {});
    },

    async saveProfile() {
      this.savingProfile = true;
      try {
        var payload = this.normalizeProfile(this.profile);
        if (payload.schedule_hour_local < 0 || payload.schedule_hour_local > 23) {
          throw new Error('Daily run hour must be between 0 and 23');
        }
        await OpenFangAPI.put('/api/sales/profile', payload);
        OpenFangToast.success('Sales profile saved');
      } catch(e) {
        OpenFangToast.error(e.message || 'Failed to save profile');
      }
      this.savingProfile = false;
    },

    async autofillProfile() {
      if (!this.profileBrief || this.profileBrief.trim().length < 20) {
        OpenFangToast.error('Brief is too short. Add your company/product context first.');
        return;
      }
      this.autofillingProfile = true;
      try {
        var data = await OpenFangAPI.post('/api/sales/profile/autofill', {
          brief: this.profileBrief.trim(),
          persist: true
        });
        this.profile = this.normalizeProfile((data && data.profile) || {});
        var source = (data && data.source) || 'autofill';
        OpenFangToast.success('Profile auto-filled (' + source + ')');
        if (data && data.warnings && data.warnings.length) {
          OpenFangToast.warn(data.warnings[0], 8000);
        }
      } catch(e) {
        OpenFangToast.error(e.message || 'Autofill failed');
      }
      this.autofillingProfile = false;
    },

    async runNow() {
      this.runningNow = true;
      try {
        var data = await OpenFangAPI.post('/api/sales/run', {});
        var run = data.run || {};
        OpenFangToast.success('Lead generation completed. Inserted: ' + String(run.inserted || 0));
        await Promise.all([this.loadRuns(), this.loadLeads(), this.loadApprovals()]);
      } catch(e) {
        OpenFangToast.error(e.message || 'Sales run failed');
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
        OpenFangToast.success('Approved and sent');
        await Promise.all([this.loadApprovals(), this.loadDeliveries()]);
      } catch(e) {
        OpenFangToast.error(e.message || 'Approval failed');
      }
    },

    async reject(id) {
      var self = this;
      OpenFangToast.confirm('Reject Message', 'Reject this message draft?', async function() {
        try {
          await OpenFangAPI.post('/api/sales/approvals/' + encodeURIComponent(id) + '/reject', { reason: 'manual_reject' });
          OpenFangToast.success('Rejected');
          await self.loadApprovals();
        } catch(e) {
          OpenFangToast.error(e.message || 'Reject failed');
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
      } catch(e) {
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
          popup.document.title = 'OpenFang OAuth';
          popup.document.body.innerHTML = '<div style="font-family:Arial,sans-serif;padding:24px;line-height:1.5">Preparing OAuth login...</div>';
        }
      } catch(_) {
        popup = null;
      }
      try {
        var res = await OpenFangAPI.post('/api/auth/codex/start', {});
        this.oauth.auth_url = res.auth_url || '';
        this.oauth.state = res.state || '';
        if (!this.oauth.auth_url) {
          throw new Error('OAuth start did not return auth_url');
        }
        if (popup && !popup.closed) {
          popup.location.replace(this.oauth.auth_url);
          popup.focus();
        } else {
          OpenFangToast.warn('Popup blocked. Click \"Open OAuth Login\" below.');
        }
        OpenFangToast.info('Complete login in the opened tab, then status will refresh.');
        await this.pollOAuthUntilConnected();
      } catch(e) {
        if (popup && !popup.closed) {
          try { popup.close(); } catch(_) {}
        }
        OpenFangToast.error(e.message || 'OAuth start failed');
      }
      this.oauthBusy = false;
    },

    async submitManualCode() {
      if (!this.manualCode || !this.manualCode.trim()) {
        OpenFangToast.error('Paste the authorization code first');
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
        OpenFangToast.success('Codex OAuth connected');
      } catch(e) {
        OpenFangToast.error(e.message || 'Manual code exchange failed');
      }
      this.oauthBusy = false;
    },

    async pollOAuthUntilConnected() {
      for (var i = 0; i < 24; i++) {
        await new Promise(function(resolve) { setTimeout(resolve, 2500); });
        await this.checkOAuthStatus();
        if (this.oauth.connected) {
          this.oauth.auth_url = '';
          OpenFangToast.success('Codex OAuth connected');
          return;
        }
      }
      OpenFangToast.warn('OAuth not confirmed yet. Use "Check" after completing login.');
    },

    async importCliAuth() {
      this.oauthBusy = true;
      try {
        await OpenFangAPI.post('/api/auth/codex/import-cli', {});
        await this.checkOAuthStatus();
        OpenFangToast.success('Imported Codex CLI auth');
      } catch(e) {
        OpenFangToast.error(e.message || 'Import failed');
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
          issued_at: null,
          expires_at: null,
          has_refresh_token: false,
          auth_url: '',
          state: ''
        };
        this.manualCode = '';
        OpenFangToast.success('Codex OAuth disconnected');
      } catch(e) {
        OpenFangToast.error(e.message || 'Logout failed');
      }
      this.oauthBusy = false;
    },

    formatDateTime(value) {
      if (!value) return '-';
      try {
        return new Date(value).toLocaleString();
      } catch(_) {
        return value;
      }
    }
  };
}
