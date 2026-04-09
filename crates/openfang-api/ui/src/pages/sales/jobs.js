'use strict';

import { OpenFangAPI, OpenFangToast } from '../../core/api.js';

export const salesJobMixins = {
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
          if (data && data.job_id) {
            self.currentJobId = data.job_id;
            self.persistCurrentJobId(data.job_id);
          }
          if (!data || data.status === 'completed' || data.status === 'failed') {
            self.stopJobPolling();
            self.currentJobId = jobId;
            await Promise.all([
              self.loadRuns(),
              self.loadProspects(),
              self.loadSourceHealth(),
              self.loadOnboardingStatus()
            ]);
            if (!self.isB2C) {
              await Promise.all([self.loadLeads(), self.loadApprovals(), self.loadDeliveries()]);
            }
            var latestRun = self.runs.length ? self.runs[0] : null;
            if (latestRun && latestRun.id) {
              if (self.isB2C) {
                await self.loadRunProspects(latestRun.id);
              } else {
                await Promise.all([
                  self.loadRunProspects(latestRun.id),
                  self.loadRunLeads(latestRun.id)
                ]);
              }
            }
            if (data && data.status === 'completed') {
              OpenFangToast.success(self.isB2C ? 'Social discovery job tamamlandi' : 'Prospecting job tamamlandi');
            } else if (data && data.error_message) {
              OpenFangToast.error(data.error_message);
            } else {
              OpenFangToast.error(self.isB2C ? 'Social discovery job basarisiz' : 'Prospecting job basarisiz');
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

    currentJobStageRecord() {
      if (!this.jobProgress || !Array.isArray(this.jobProgress.stages)) return null;
      var current = this.jobProgress.current_stage || '';
      var stages = this.jobProgress.stages;
      for (var i = 0; i < stages.length; i++) {
        if (stages[i] && stages[i].name === current) return stages[i];
      }
      return stages.length ? stages[stages.length - 1] : null;
    },

    jobProgressPercent() {
      if (!this.jobProgress || !Array.isArray(this.jobProgress.stages) || !this.jobProgress.stages.length) {
        return this.jobProgress && this.jobProgress.status === 'completed' ? 100 : 0;
      }
      var stages = this.jobProgress.stages;
      var completed = 0;
      for (var i = 0; i < stages.length; i++) {
        if (stages[i] && stages[i].status === 'completed') completed += 1;
      }
      var currentStage = this.currentJobStageRecord();
      var checkpoint = currentStage && currentStage.checkpoint ? currentStage.checkpoint : null;
      if (
        currentStage &&
        currentStage.status === 'running' &&
        checkpoint &&
        typeof checkpoint.total_candidates === 'number' &&
        checkpoint.total_candidates > 0 &&
        typeof checkpoint.processed_candidates === 'number'
      ) {
        var stageFraction = checkpoint.processed_candidates / checkpoint.total_candidates;
        if (stageFraction < 0) stageFraction = 0;
        if (stageFraction > 1) stageFraction = 1;
        return Math.round(((completed + stageFraction) / stages.length) * 100);
      }
      return Math.round((completed / stages.length) * 100);
    },

    jobProgressSummary() {
      var stage = this.currentJobStageRecord();
      var checkpoint = stage && stage.checkpoint ? stage.checkpoint : null;
      if (checkpoint) {
        var parts = [];
        if (
          typeof checkpoint.processed_candidates === 'number' &&
          typeof checkpoint.total_candidates === 'number' &&
          checkpoint.total_candidates > 0
        ) {
          parts.push(String(checkpoint.processed_candidates) + '/' + String(checkpoint.total_candidates) + ' aday isleme alindi');
        }
        if (typeof checkpoint.profiled_accounts === 'number') {
          parts.push(String(checkpoint.profiled_accounts) + (this.isB2C ? ' social profile kaydedildi' : ' dossier kaydedildi'));
        }
        if (typeof checkpoint.inserted === 'number') {
          if (!this.isB2C) {
            parts.push(String(checkpoint.inserted) + ' lead uretildi');
          }
        }
        if (typeof checkpoint.approvals_queued === 'number' && checkpoint.approvals_queued > 0) {
          parts.push(String(checkpoint.approvals_queued) + ' onay kuyruga alindi');
        }
        if (checkpoint.current_domain) {
          parts.push('Siradaki domain: ' + checkpoint.current_domain);
        }
        if (parts.length) return parts.join(' | ');
      }
      if (this.jobProgress && this.jobProgress.status === 'completed') {
        return this.isB2C
          ? 'Job tamamlandi. Social profile listesi yenileniyor.'
          : 'Job tamamlandi. Run, prospect ve lead listeleri yenileniyor.';
      }
      if (this.jobProgress && this.jobProgress.status === 'failed') {
        return this.jobProgress.error_message || 'Job basarisiz tamamlandi.';
      }
      return '';
    },

    jobStageNote(stage) {
      if (!stage || !stage.checkpoint) return '-';
      var checkpoint = stage.checkpoint;
      var parts = [];
      if (
        typeof checkpoint.processed_candidates === 'number' &&
        typeof checkpoint.total_candidates === 'number' &&
        checkpoint.total_candidates > 0
      ) {
        parts.push(String(checkpoint.processed_candidates) + '/' + String(checkpoint.total_candidates));
      }
      if (checkpoint.current_domain) {
        parts.push(checkpoint.current_domain);
      }
      if (typeof checkpoint.inserted === 'number') {
        if (!this.isB2C) {
          parts.push('lead ' + String(checkpoint.inserted));
        }
      }
      return parts.length ? parts.join(' | ') : '-';
    },
    async retryJob(forceFresh) {
      if (!this.currentJobId) {
        OpenFangToast.error('Tekrar denenecek job bulunamadi');
        return;
      }
      this.runningNow = true;
      try {
        var data = await OpenFangAPI.post(
          this.buildSalesUrl('/api/sales/jobs/' + encodeURIComponent(this.currentJobId) + '/retry'),
          { force_fresh: !!forceFresh }
        );
        var jobId = (data && data.job_id) || '';
        if (!jobId) throw new Error('Retry job_id donmedi');
        this.currentJobId = jobId;
        this.persistCurrentJobId(jobId);
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
};
