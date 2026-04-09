'use strict';

import { PulsivoSalesmanAPI, PulsivoSalesmanToast } from '../../core/api.js';

export const salesOauthMixins = {
    async checkOAuthStatus() {
      this.oauthBusy = true;
      try {
        var status = await PulsivoSalesmanAPI.get('/api/auth/codex/status');
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
        var res = await PulsivoSalesmanAPI.post('/api/auth/codex/start', {});
        this.oauth.auth_url = res.auth_url || '';
        this.oauth.state = res.state || '';
        if (!this.oauth.auth_url) throw new Error('OAuth baslatilamadi: auth_url donmedi');

        if (popup && !popup.closed) {
          popup.location.replace(this.oauth.auth_url);
          popup.focus();
        } else {
          PulsivoSalesmanToast.warn('Popup engellendi. Alttaki "OAuth Girisini Ac" baglantisini kullanin.');
        }

        await this.pollOAuthUntilConnected();
        await this.loadOnboardingStatus();
      } catch (e) {
        if (popup && !popup.closed) {
          try { popup.close(); } catch (_) {}
        }
        PulsivoSalesmanToast.error(e && e.message ? e.message : 'OAuth baslatilamadi');
      }
      this.oauthBusy = false;
    },

    async submitManualCode() {
      if (!this.manualCode || !this.manualCode.trim()) {
        PulsivoSalesmanToast.error('Lutfen OAuth kodunu yapistirin');
        return;
      }
      this.oauthBusy = true;
      try {
        await PulsivoSalesmanAPI.post('/api/auth/codex/paste-code', {
          code: this.manualCode.trim(),
          state: this.oauth.state || undefined
        });
        this.manualCode = '';
        await this.checkOAuthStatus();
        await this.loadOnboardingStatus();
        PulsivoSalesmanToast.success('Codex OAuth baglandi');
      } catch (e) {
        PulsivoSalesmanToast.error(e && e.message ? e.message : 'Kod degisimi basarisiz');
      }
      this.oauthBusy = false;
    },

    async pollOAuthUntilConnected() {
      for (var i = 0; i < 24; i++) {
        await new Promise(function(resolve) { setTimeout(resolve, 2500); });
        await this.checkOAuthStatus();
        if (this.oauth.connected) {
          this.oauth.auth_url = '';
          PulsivoSalesmanToast.success('Codex OAuth baglandi');
          return;
        }
      }
      PulsivoSalesmanToast.warn('OAuth henuz dogrulanmadi. Giris tamamlandiysa "Durum Kontrol" butonuna basin.');
    },

    async importCliAuth() {
      this.oauthBusy = true;
      try {
        await PulsivoSalesmanAPI.post('/api/auth/codex/import-cli', {});
        await this.checkOAuthStatus();
        await this.loadOnboardingStatus();
        PulsivoSalesmanToast.success('~/.codex/auth.json ice aktarildi');
      } catch (e) {
        PulsivoSalesmanToast.error(e && e.message ? e.message : 'Ice aktarma basarisiz');
      }
      this.oauthBusy = false;
    },

    async logoutOAuth() {
      this.oauthBusy = true;
      try {
        await PulsivoSalesmanAPI.post('/api/auth/codex/logout', {});
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
        PulsivoSalesmanToast.success('Codex OAuth baglantisi kesildi');
      } catch (e) {
        PulsivoSalesmanToast.error(e && e.message ? e.message : 'Cikis islemi basarisiz');
      }
      this.oauthBusy = false;
    },
};
