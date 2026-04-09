'use strict';

export function createSalesState() {
  return {
    loading: true,
    loadError: '',
    savingProfile: false,
    autofillingProfile: false,
    runningNow: false,
    oauthBusy: false,
    currentJobId: '',
    currentPage: 'prospecting-b2b',
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
  };
}

export const salesStateMixins = {
    routePage() {
      var raw = (window.location.hash || '').replace('#', '').trim().toLowerCase();
      var page = raw.split('/')[0];
      if (!page || page === 'prospecting') return 'prospecting-b2b';
      if (page === 'prospecting-b2c') return 'prospecting-b2c';
      return 'prospecting-b2b';
    },

    get isB2C() {
      return this.routePage() === 'prospecting-b2c';
    },

    get segmentKey() {
      return this.isB2C ? 'b2c' : 'b2b';
    },

    currentJobStorageKey() {
      return 'openfang-sales-current-job:' + this.segmentKey;
    },

    buildSalesUrl(path, params) {
      var parts = [];
      var source = params || {};
      var keys = Object.keys(source);
      for (var i = 0; i < keys.length; i++) {
        if (source[keys[i]] === undefined || source[keys[i]] === null || source[keys[i]] === '') continue;
        parts.push(encodeURIComponent(keys[i]) + '=' + encodeURIComponent(String(source[keys[i]])));
      }
      parts.push('segment=' + encodeURIComponent(this.segmentKey));
      return path + (path.indexOf('?') >= 0 ? '&' : '?') + parts.join('&');
    },

    profileDefaults() {
      if (this.isB2C) {
        return {
          product_name: '',
          product_description: '',
          target_industry: 'Local Consumer',
          target_geo: 'TR',
          sender_name: '',
          sender_email: '',
          sender_linkedin: '',
          target_title_policy: 'ceo_then_founder',
          daily_target: 40,
          daily_send_cap: 5,
          schedule_hour_local: 10,
          timezone_mode: 'local'
        };
      }
      return {
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
      };
    },

    get showOnboarding() {
      return !this.onboarding.completed;
    },

    get tabItems() {
      var items = [
        { key: 'command', label: 'Command Center' },
        { key: 'profiles', label: 'Profiles' }
      ];
      if (!this.isB2C) {
        items.push({ key: 'approvals', label: 'Approval Queue' });
        items.push({ key: 'deliveries', label: 'Delivery' });
      }
      return items;
    },

    get modeLabel() {
      return this.isB2C ? 'B2C Discovery' : 'B2B Discovery';
    },

    get onboardingTitle() {
      return this.isB2C ? 'B2C Discovery Kurulumu' : 'Prospecting Harness Kurulumu';
    },

    get onboardingIntro() {
      return this.isB2C
        ? 'Amac: Instagram, TikTok ve lokal pazar sinyalleriyle bireysel tuketici profillerini bulmak ve zenginlestirmek.'
        : "Amac: ICP'nize uyan aday musteri hesaplarini bulmak, profillemek ve action-ready hale getirmek.";
    },

    get briefLabel() {
      return this.isB2C ? 'Marka / urun / niche briefi' : 'Sirket briefi';
    },

    get briefHelpText() {
      return this.isB2C
        ? 'Marka, hedef tuketici, sehir/bolge, ilgi alanlari ve sosyal platform odaklarini tek seferde yapistirin.'
        : 'Sirket/urun/ICP/sektor/iletisim bilgilerini tek seferde yapistirin.';
    },

    get briefPlaceholder() {
      return this.isB2C
        ? 'Markanizin, urununuzun ve hedef tuketici profilinizin briefini buraya yapistirin...'
        : 'Sirketinizin briefini buraya yapistirin...';
    },

    get runActionLabel() {
      return this.isB2C ? 'Yeni social discovery run al' : 'Yeni run al';
    },

    get firstRunLabel() {
      return this.isB2C ? "Ilk social discovery run'i" : "Ilk aday musteri profili run'i";
    },

    get firstRunActionLabel() {
      return this.isB2C ? "Ilk social run'i baslat" : "Ilk run'i baslat";
    },

    get firstRunHelp() {
      return this.isB2C
        ? 'En az 1 anlamli social/local profile cikinca onboarding tamamlanir.'
        : 'En az 1 anlamli prospect profile cikinca onboarding tamamlanir.';
    },

    get profilesTitle() {
      return this.isB2C ? 'Tuketici social profilleri' : 'Aday musteri profilleri';
    },

    get selectedEntityLabel() {
      return this.isB2C ? 'Secili profile' : 'Secili account';
    },

    get runHistoryDiscoveryLabel() {
      return this.isB2C ? 'Profile kesfi' : 'Profil kesfi';
    },

    get runHistoryActionLabel() {
      return this.isB2C ? 'DM-ready' : 'Action-ready';
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

    get selectedProspectSignalCount() {
      var prospect = this.selectedProspectRecord();
      return prospect && Array.isArray(prospect.matched_signals) ? prospect.matched_signals.length : 0;
    },

    get selectedProspectLinkCount() {
      return this.prospectOsintLinks(this.selectedProspectRecord()).length;
    },

    get selectedAccountLabel() {
      var dossier = this.selectedDossier;
      if (dossier && dossier.account && dossier.account.display_name) return dossier.account.display_name;
      var prospect = this.selectedProspectRecord();
      return prospect ? prospect.company : this.selectedEntityLabel;
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
      var defaults = this.profileDefaults();
      var src = p || {};
      return {
        product_name: src.product_name || defaults.product_name,
        product_description: src.product_description || defaults.product_description,
        target_industry: src.target_industry || defaults.target_industry,
        target_geo: src.target_geo || defaults.target_geo,
        sender_name: src.sender_name || defaults.sender_name,
        sender_email: src.sender_email || defaults.sender_email,
        sender_linkedin: src.sender_linkedin || defaults.sender_linkedin,
        target_title_policy: src.target_title_policy || defaults.target_title_policy,
        daily_target: Number(src.daily_target || defaults.daily_target),
        daily_send_cap: Number(src.daily_send_cap || defaults.daily_send_cap),
        schedule_hour_local: Number(src.schedule_hour_local || defaults.schedule_hour_local),
        timezone_mode: src.timezone_mode || defaults.timezone_mode
      };
    },
};
