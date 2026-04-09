'use strict';

import { PulsivoSalesmanAPI, PulsivoSalesmanToast } from '../../core/api.js';

function nonEmpty(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function truncateText(value, maxLen) {
  var text = nonEmpty(value).replace(/\s+/g, ' ');
  if (!text || text.length <= maxLen) return text;
  return text.slice(0, Math.max(0, maxLen - 3)).trim() + '...';
}

function parseJson(value) {
  if (typeof value !== 'string' || !value.trim()) return null;
  try {
    return JSON.parse(value);
  } catch (_) {
    return null;
  }
}

export const salesOpsMixins = {
    resetOpsState() {
      this.policyProposals = [];
      this.contextFactors = [];
      this.contextSummary = {
        bad_timing_today: false,
        current_budget_quarter: ''
      };
      this.calibrationProposals = [];
      this.calibrating = false;
      this.advancingSequences = false;
      this.experiments = [];
      this.selectedExperimentId = '';
      this.experimentResults = null;
      this.creatingExperiment = false;
      this.experimentForm = this.defaultExperimentForm();
    },

    async loadOpsData() {
      await Promise.all([
        this.loadPolicyProposals(),
        this.loadContextFactors(),
        this.loadExperiments()
      ]);
    },

    async loadPolicyProposals() {
      var data = await PulsivoSalesmanAPI.get('/api/sales/policy-proposals?limit=100');
      this.policyProposals = Array.isArray(data && data.proposals) ? data.proposals : [];
    },

    async approvePolicyProposal(id) {
      if (!id) return;
      try {
        await PulsivoSalesmanAPI.post('/api/sales/policy-proposals/' + encodeURIComponent(id) + '/approve', {});
        await this.loadPolicyProposals();
        PulsivoSalesmanToast.success('Policy onerisi aktif edildi');
      } catch (e) {
        PulsivoSalesmanToast.error(e && e.message ? e.message : 'Policy onerisi aktif edilemedi');
      }
    },

    async rejectPolicyProposal(id) {
      if (!id) return;
      try {
        await PulsivoSalesmanAPI.post('/api/sales/policy-proposals/' + encodeURIComponent(id) + '/reject', {});
        await this.loadPolicyProposals();
        PulsivoSalesmanToast.success('Policy onerisi retired olarak isaretlendi');
      } catch (e) {
        PulsivoSalesmanToast.error(e && e.message ? e.message : 'Policy onerisi guncellenemedi');
      }
    },

    async loadContextFactors() {
      var data = await PulsivoSalesmanAPI.get('/api/sales/context-factors');
      this.contextFactors = Array.isArray(data && data.factors) ? data.factors : [];
      this.contextSummary = {
        bad_timing_today: !!(data && data.bad_timing_today),
        current_budget_quarter: (data && data.current_budget_quarter) || ''
      };
    },

    async runCalibration() {
      this.calibrating = true;
      try {
        var data = await PulsivoSalesmanAPI.post('/api/sales/calibration/run', {});
        this.calibrationProposals = Array.isArray(data && data.proposals) ? data.proposals : [];
        await this.loadPolicyProposals();
        PulsivoSalesmanToast.success(
          this.calibrationProposals.length
            ? (String(this.calibrationProposals.length) + ' calibration onerisi uretildi')
            : 'Calibration tamamlandi, yeni oneri cikmadi'
        );
      } catch (e) {
        PulsivoSalesmanToast.error(e && e.message ? e.message : 'Calibration calistirilamadi');
      }
      this.calibrating = false;
    },

    async advanceSequences() {
      this.advancingSequences = true;
      try {
        var data = await PulsivoSalesmanAPI.post('/api/sales/sequences/advance', {});
        var advanced = Number(data && data.advanced ? data.advanced : 0);
        await Promise.all([
          this.loadApprovals(),
          this.loadDeliveries(),
          this.loadExperiments()
        ]);
        PulsivoSalesmanToast.success(
          advanced > 0
            ? (String(advanced) + ' sequence ilerletildi')
            : 'Sequence advance calisti, ilerletilecek kayit yoktu'
        );
      } catch (e) {
        PulsivoSalesmanToast.error(e && e.message ? e.message : 'Sequence advance calistirilamadi');
      }
      this.advancingSequences = false;
    },

    ensureSelectedExperiment() {
      var current = this.selectedExperimentRecord;
      if (current && current.id) {
        this.selectedExperimentId = current.id;
        return;
      }
      this.selectedExperimentId = this.experiments.length ? this.experiments[0].id : '';
    },

    async loadExperiments() {
      var data = await PulsivoSalesmanAPI.get('/api/sales/experiments');
      this.experiments = Array.isArray(data && data.experiments) ? data.experiments : [];
      this.ensureSelectedExperiment();
      if (this.selectedExperimentId) {
        await this.loadExperimentResults(this.selectedExperimentId);
      } else {
        this.experimentResults = null;
      }
    },

    async loadExperimentResults(id) {
      if (!id) {
        this.experimentResults = null;
        return;
      }
      this.selectedExperimentId = id;
      try {
        this.experimentResults = await PulsivoSalesmanAPI.get(
          '/api/sales/experiments/' + encodeURIComponent(id) + '/results'
        );
      } catch (e) {
        this.experimentResults = null;
        PulsivoSalesmanToast.error(e && e.message ? e.message : 'Experiment sonuclari alinamadi');
      }
    },

    async createExperiment() {
      var form = this.experimentForm || this.defaultExperimentForm();
      if (!nonEmpty(form.name)) {
        PulsivoSalesmanToast.error('Experiment adi zorunlu');
        return;
      }
      if (!nonEmpty(form.variant_a) || !nonEmpty(form.variant_b)) {
        PulsivoSalesmanToast.error('Iki varyant da zorunlu');
        return;
      }
      this.creatingExperiment = true;
      try {
        var data = await PulsivoSalesmanAPI.post('/api/sales/experiments', {
          name: nonEmpty(form.name),
          hypothesis: nonEmpty(form.hypothesis),
          variant_a: nonEmpty(form.variant_a),
          variant_b: nonEmpty(form.variant_b)
        });
        this.experimentForm = this.defaultExperimentForm();
        await this.loadExperiments();
        if (data && data.id) {
          await this.loadExperimentResults(data.id);
        }
        PulsivoSalesmanToast.success('Experiment acildi');
      } catch (e) {
        PulsivoSalesmanToast.error(e && e.message ? e.message : 'Experiment olusturulamadi');
      }
      this.creatingExperiment = false;
    },

    async reAutofillProfile() {
      if (!this.profileBrief || this.profileBrief.trim().length < 20) {
        PulsivoSalesmanToast.error('Autofill icin kayitli brief cok kisa');
        return;
      }
      this.autofillingProfile = true;
      try {
        var data = await PulsivoSalesmanAPI.post(
          this.buildSalesUrl('/api/sales/profile/autofill'),
          {
            brief: this.profileBrief.trim(),
            persist: true
          }
        );
        this.profile = this.normalizeProfile((data && data.profile) || {});
        if (data && data.onboarding) {
          this.onboarding = data.onboarding;
        } else {
          await this.loadOnboardingStatus();
        }
        PulsivoSalesmanToast.success(
          'Profil brief uzerinden yeniden dolduruldu (' + ((data && data.source) || 'autofill') + ')'
        );
      } catch (e) {
        PulsivoSalesmanToast.error(e && e.message ? e.message : 'Profil yeniden doldurulamadi');
      }
      this.autofillingProfile = false;
    },

    policyStatusClass(status) {
      if (status === 'active') return 'badge-success';
      if (status === 'proposed') return 'badge-warn';
      if (status === 'retired') return 'badge-muted';
      return 'badge-info';
    },

    policyChangeSummary(proposal) {
      if (!proposal) return '-';
      var payload = parseJson(proposal.new_value);
      if (payload && payload.driver_signal_id) {
        return [
          payload.signal_type || proposal.rule_key,
          payload.direction || 'review',
          payload.driver_signal_id
        ].join(' / ');
      }
      return truncateText(proposal.new_value || '-', 120);
    },

    policyEvidenceSummary(proposal) {
      if (!proposal) return '-';
      var backtest = parseJson(proposal.backtest_result_json);
      if (backtest) {
        var parts = [];
        if (typeof backtest.validated_signal_count === 'number') {
          parts.push('validated ' + String(backtest.validated_signal_count));
        }
        if (typeof backtest.false_positive_signal_count === 'number') {
          parts.push('false+ ' + String(backtest.false_positive_signal_count));
        }
        if (typeof backtest.missed_signal_count === 'number') {
          parts.push('missed ' + String(backtest.missed_signal_count));
        }
        if (parts.length) return parts.join(' | ');
      }
      return truncateText(proposal.old_value || proposal.proposal_source || '-', 120);
    },

    contextFactorClass(factor) {
      if (!factor) return 'badge-muted';
      if (factor.factor_type === 'timing' || factor.factor_type === 'blackout') return 'badge-warn';
      if (factor.factor_type === 'budget' || factor.factor_type === 'quarter') return 'badge-info';
      return 'badge-muted';
    },

    experimentStatusClass(status) {
      if (status === 'active') return 'badge-success';
      if (status === 'paused') return 'badge-warn';
      if (status === 'completed') return 'badge-info';
      return 'badge-muted';
    },

    experimentVariantRows() {
      var rows = this.experimentResults && Array.isArray(this.experimentResults.variants)
        ? this.experimentResults.variants.slice()
        : [];
      return rows.sort(function(left, right) {
        return String(left.variant || '').localeCompare(String(right.variant || ''));
      });
    },

    experimentLeadLabel() {
      var rows = this.experimentVariantRows();
      if (!rows.length) return 'Sonuc bekleniyor';
      rows.sort(function(left, right) {
        return Number(right.positive_rate || 0) - Number(left.positive_rate || 0);
      });
      var lead = rows[0];
      if (!lead || Number(lead.total_outcomes || 0) === 0) return 'Sonuc bekleniyor';
      return 'Lider varyant: ' + String(lead.variant || '-').toUpperCase();
    }
};
