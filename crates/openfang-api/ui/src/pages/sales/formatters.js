'use strict';

export const salesFormatterMixins = {
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
    asPercent(value) {
      if (typeof value !== 'number' || isNaN(value)) return '-';
      return String(Math.round(value * 100)) + '%';
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
    formatDateTime(value) {
      if (!value) return '-';
      try {
        return new Date(value).toLocaleString('tr-TR');
      } catch (_) {
        return value;
      }
    }
};
