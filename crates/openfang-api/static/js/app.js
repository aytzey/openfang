'use strict';

function app() {
  return {
    page: 'sales',
    themeMode: localStorage.getItem('openfang-theme-mode') || 'system',
    theme: (() => {
      var mode = localStorage.getItem('openfang-theme-mode') || 'system';
      if (mode === 'system') {
        return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
      }
      return mode;
    })(),
    connected: false,
    wsConnected: false,
    connectionState: 'connected',
    lastError: '',

    init() {
      var self = this;

      function enforceSalesHash() {
        var hash = (window.location.hash || '').replace('#', '').trim().toLowerCase();
        if (!hash || hash !== 'sales') {
          window.location.hash = 'sales';
        }
        self.page = 'sales';
      }

      window.addEventListener('hashchange', enforceSalesHash);
      enforceSalesHash();

      window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function(e) {
        if (self.themeMode === 'system') {
          self.theme = e.matches ? 'dark' : 'light';
        }
      });

      OpenFangAPI.onConnectionChange(function(state) {
        self.connectionState = state;
      });

      this.pollStatus();
      setInterval(function() { self.pollStatus(); }, 5000);
    },

    async pollStatus() {
      try {
        await OpenFangAPI.get('/api/status');
        this.connected = true;
        this.wsConnected = OpenFangAPI.isWsConnected();
        this.lastError = '';
      } catch (e) {
        this.connected = false;
        this.wsConnected = false;
        this.lastError = e && e.message ? e.message : 'Baglanti hatasi';
      }
    },

    setTheme(mode) {
      this.themeMode = mode;
      localStorage.setItem('openfang-theme-mode', mode);
      if (mode === 'system') {
        this.theme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
      } else {
        this.theme = mode;
      }
    }
  };
}
