'use strict';

import { PulsivoSalesmanAPI } from './api.js';

export function createApp() {
  return {
    page: 'prospecting-b2b',
    themeMode: localStorage.getItem('pulsivo-salesman-theme-mode') || 'system',
    theme: (() => {
      var mode = localStorage.getItem('pulsivo-salesman-theme-mode') || 'system';
      if (mode === 'system') {
        return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
      }
      return mode;
    })(),
    connected: false,
    connectionState: 'connected',
    lastError: '',

    normalizePage(hashValue) {
      var raw = (hashValue || '').replace('#', '').trim().toLowerCase();
      var page = raw.split('/')[0];
      if (!page || page === 'prospecting') return 'prospecting-b2b';
      if (page === 'prospecting-b2b' || page === 'prospecting-b2c') return page;
      return 'prospecting-b2b';
    },

    get pageLabel() {
      return this.page === 'prospecting-b2c'
        ? 'B2C Growth OS'
        : 'Prospecting Harness / B2B';
    },

    init() {
      var self = this;

      function enforcePageHash() {
        var normalized = self.normalizePage(window.location.hash || '');
        var rest = ((window.location.hash || '').replace('#', '').trim().split('/').slice(1).join('/'));
        self.page = normalized;
        if (!window.location.hash || self.normalizePage(window.location.hash || '') !== normalized) {
          window.location.hash = normalized + (rest ? ('/' + rest) : '');
          return;
        }
      }

      window.addEventListener('hashchange', enforcePageHash);
      enforcePageHash();

      window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function(e) {
        if (self.themeMode === 'system') {
          self.theme = e.matches ? 'dark' : 'light';
        }
      });

      PulsivoSalesmanAPI.onConnectionChange(function(state) {
        self.connectionState = state;
      });

      this.pollStatus();
      setInterval(function() { self.pollStatus(); }, 5000);
    },

    goToPage(page) {
      var normalized = this.normalizePage(page);
      var current = (window.location.hash || '').replace('#', '').trim();
      var rest = current.split('/').slice(1).join('/');
      window.location.hash = normalized + (rest ? ('/' + rest) : '');
    },

    async pollStatus() {
      try {
        await PulsivoSalesmanAPI.get('/api/health');
        this.connected = true;
        this.lastError = '';
      } catch (e) {
        this.connected = false;
        this.lastError = e && e.message ? e.message : 'Baglanti hatasi';
      }
    },

    setTheme(mode) {
      this.themeMode = mode;
      localStorage.setItem('pulsivo-salesman-theme-mode', mode);
      if (mode === 'system') {
        this.theme = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
      } else {
        this.theme = mode;
      }
    }
  };
}
