'use strict';

function defaultExperimentForm() {
  return {
    name: '',
    hypothesis: '',
    variant_a: 'control',
    variant_b: 'treatment'
  };
}

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
    policyProposals: [],
    contextFactors: [],
    contextSummary: {
      bad_timing_today: false,
      current_budget_quarter: ''
    },
    calibrationProposals: [],
    calibrating: false,
    advancingSequences: false,
    experiments: [],
    selectedExperimentId: '',
    experimentResults: null,
    creatingExperiment: false,
    experimentForm: defaultExperimentForm(),

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
      return 'pulsivo-salesman-sales-current-job:' + this.segmentKey;
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
        { key: 'command', label: this.isB2C ? 'Command' : 'Command Center' }
      ];
      if (this.isB2C) {
        items.push({ key: 'market', label: 'Market' });
      }
      items.push({ key: 'profiles', label: this.isB2C ? 'Buyers' : 'Profiles' });
      if (!this.isB2C) {
        items.push({ key: 'approvals', label: 'Approval Queue' });
        items.push({ key: 'deliveries', label: 'Delivery' });
        items.push({ key: 'ops', label: 'Ops Lab' });
      }
      return items;
    },

    get modeLabel() {
      return this.isB2C ? 'B2C Growth Command' : 'B2B Discovery';
    },

    get onboardingTitle() {
      return this.isB2C ? 'B2C Growth Setup' : 'Prospecting Harness Kurulumu';
    },

    get onboardingIntro() {
      return this.isB2C
        ? 'Amac: niche, teklif ve sosyal/lokal baglami tek hatta toplayip B2C buyer havuzunu gercek satis aksiyonuna cevirmek.'
        : "Amac: ICP'nize uyan aday musteri hesaplarini bulmak, profillemek ve action-ready hale getirmek.";
    },

    get briefLabel() {
      return this.isB2C ? 'Marka / teklif / niche briefi' : 'Sirket briefi';
    },

    get briefHelpText() {
      return this.isB2C
        ? 'Marka, hero urun, fiyat bandi, hedef sehir, sosyal proof, teklif vaadi ve hangi buyer tipini istediginizi tek blokta verin.'
        : 'Sirket/urun/ICP/sektor/iletisim bilgilerini tek seferde yapistirin.';
    },

    get briefPlaceholder() {
      return this.isB2C
        ? 'Marka, hero SKU, fiyat bandi, hedef sehir, vaadiniz, proof assetleriniz, istenen buyer tipi, Instagram/TikTok odagi...'
        : 'Sirketinizin briefini buraya yapistirin...';
    },

    get runActionLabel() {
      return this.isB2C ? 'Yeni pazar taramasi al' : 'Yeni run al';
    },

    get firstRunLabel() {
      return this.isB2C ? "Ilk buyer-market run'i" : "Ilk aday musteri profili run'i";
    },

    get firstRunActionLabel() {
      return this.isB2C ? "Ilk market run'ini baslat" : "Ilk run'i baslat";
    },

    get firstRunHelp() {
      return this.isB2C
        ? 'En az 1 anlamli buyer pocket cikinca onboarding tamamlanir; ideal hedef 5+ kaliteli profile ulasmak.'
        : 'En az 1 anlamli prospect profile cikinca onboarding tamamlanir.';
    },

    get profilesTitle() {
      return this.isB2C ? 'High-intent buyer profilleri' : 'Aday musteri profilleri';
    },

    get selectedEntityLabel() {
      return this.isB2C ? 'Secili buyer' : 'Secili account';
    },

    get runHistoryDiscoveryLabel() {
      return this.isB2C ? 'Profile kesfi' : 'Profil kesfi';
    },

    get runHistoryActionLabel() {
      return this.isB2C ? 'DM-ready profile' : 'Action-ready';
    },

    get pendingApprovals() {
      return this.approvals.filter(function(a) { return a.status === 'pending'; }).length;
    },

    get pendingApprovalItems() {
      return this.approvals.filter(function(a) { return a.status === 'pending'; });
    },

    get contactReadyProspects() {
      if (this.isB2C && typeof this.b2cHighIntentCount === 'function') {
        return this.b2cHighIntentCount();
      }
      return this.prospects.filter(function(p) { return p.profile_status === 'contact_ready'; }).length;
    },

    get companyOnlyProspects() {
      if (this.isB2C && typeof this.b2cResearchBacklogCount === 'function') {
        return this.b2cResearchBacklogCount();
      }
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

    get proposedPolicyCount() {
      var items = this.policyProposals || [];
      var count = 0;
      for (var i = 0; i < items.length; i++) {
        if (items[i] && items[i].status === 'proposed') count += 1;
      }
      return count;
    },

    get activeExperimentCount() {
      var items = this.experiments || [];
      var count = 0;
      for (var i = 0; i < items.length; i++) {
        if (items[i] && items[i].status === 'active') count += 1;
      }
      return count;
    },

    get selectedExperimentRecord() {
      var items = this.experiments || [];
      for (var i = 0; i < items.length; i++) {
        if (items[i] && items[i].id === this.selectedExperimentId) return items[i];
      }
      return items.length ? items[0] : null;
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

    defaultExperimentForm() {
      return defaultExperimentForm();
    },
};
