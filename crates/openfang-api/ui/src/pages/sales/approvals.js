'use strict';

import { OpenFangAPI, OpenFangToast } from '../../core/api.js';

export const salesApprovalMixins = {
    approvalSelected(id) {
      return !!(this.approvalSelections && this.approvalSelections[id]);
    },

    toggleApprovalSelection(id, checked) {
      if (!id) return;
      this.approvalSelections[id] = !!checked;
    },

    toggleAllPendingApprovals(checked) {
      var items = this.pendingApprovalItems;
      for (var i = 0; i < items.length; i++) {
        this.approvalSelections[items[i].id] = !!checked;
      }
    },

    pruneApprovalState() {
      var keep = {};
      var ids = {};
      for (var i = 0; i < this.pendingApprovalItems.length; i++) {
        ids[this.pendingApprovalItems[i].id] = true;
      }
      var selectionIds = Object.keys(this.approvalSelections || {});
      for (var j = 0; j < selectionIds.length; j++) {
        if (ids[selectionIds[j]]) keep[selectionIds[j]] = !!this.approvalSelections[selectionIds[j]];
      }
      this.approvalSelections = keep;

      var draftKeep = {};
      var draftIds = Object.keys(this.approvalDrafts || {});
      for (var k = 0; k < draftIds.length; k++) {
        if (ids[draftIds[k]]) draftKeep[draftIds[k]] = this.approvalDrafts[draftIds[k]];
      }
      this.approvalDrafts = draftKeep;

      if (this.approvalEditorId && !ids[this.approvalEditorId]) {
        this.approvalEditorId = '';
      }
    },

    syncApprovalCursor() {
      var items = this.pendingApprovalItems;
      if (!items.length) {
        this.approvalCursorId = '';
        return;
      }
      for (var i = 0; i < items.length; i++) {
        if (items[i].id === this.approvalCursorId) return;
      }
      this.approvalCursorId = items[0].id;
    },

    moveApprovalCursor(delta) {
      var items = this.pendingApprovalItems;
      if (!items.length) {
        this.approvalCursorId = '';
        return;
      }
      var idx = 0;
      for (var i = 0; i < items.length; i++) {
        if (items[i].id === this.approvalCursorId) {
          idx = i;
          break;
        }
      }
      idx = (idx + delta + items.length) % items.length;
      this.approvalCursorId = items[idx].id;
    },

    currentPendingApproval() {
      var items = this.pendingApprovalItems;
      for (var i = 0; i < items.length; i++) {
        if (items[i].id === this.approvalCursorId) return items[i];
      }
      return items.length ? items[0] : null;
    },

    approvalRowClass(approval) {
      return approval && approval.id === this.approvalCursorId ? 'is-active' : '';
    },

    approvalClassification(approval) {
      return approval && approval.payload && approval.payload.classification
        ? approval.payload.classification
        : '';
    },

    approvalHasWarning(approval) {
      var cls = this.approvalClassification(approval);
      return cls === 'generic' || cls === 'role' || cls === 'consumer';
    },

    approvalWarningLabel(approval) {
      var cls = this.approvalClassification(approval);
      if (cls === 'generic' || cls === 'role') return 'Generic email - low reply probability';
      if (cls === 'consumer') return 'Consumer mailbox - blocked by policy';
      return '';
    },

    approvalDraft(approval) {
      if (!approval || !approval.id) return {};
      if (!this.approvalDrafts[approval.id]) {
        var payload = approval.payload || {};
        this.approvalDrafts[approval.id] = {
          to: payload.to || '',
          profile_url: payload.profile_url || '',
          subject: payload.subject || '',
          body: payload.body || '',
          message: payload.message || '',
          icebreaker: this.extractIcebreaker(approval)
        };
      }
      return this.approvalDrafts[approval.id];
    },

    extractIcebreaker(approval) {
      if (!approval || !approval.payload) return '';
      var text = approval.channel === 'email' ? (approval.payload.body || '') : (approval.payload.message || '');
      if (!text) return '';
      var parts = text.split(/\n+/);
      return parts[0] || '';
    },

    approvalBodyRemainder(approval) {
      if (!approval || !approval.payload) return '';
      var text = approval.channel === 'email' ? (approval.payload.body || '') : (approval.payload.message || '');
      if (!text) return '';
      var parts = text.split(/\n+/);
      parts.shift();
      return parts.join('\n').trim();
    },

    startApprovalEdit(approval) {
      if (!approval || !approval.id) return;
      this.approvalEditorId = approval.id;
      this.approvalCursorId = approval.id;
      this.approvalDraft(approval);
    },

    cancelApprovalEdit() {
      this.approvalEditorId = '';
    },

    buildEditedPayload(approval) {
      var draft = this.approvalDraft(approval);
      if (approval.channel === 'email') {
        return {
          to: draft.to || (approval.payload && approval.payload.to) || '',
          subject: draft.subject || '',
          body: draft.body || ''
        };
      }
      return {
        profile_url: draft.profile_url || (approval.payload && approval.payload.profile_url) || '',
        message: draft.message || ''
      };
    },

    async saveApprovalEdit(id) {
      var approval = null;
      for (var i = 0; i < this.pendingApprovalItems.length; i++) {
        if (this.pendingApprovalItems[i].id === id) {
          approval = this.pendingApprovalItems[i];
          break;
        }
      }
      if (!approval) {
        OpenFangToast.error('Duzenlenecek approval bulunamadi');
        return;
      }
      try {
        var data = await OpenFangAPI.patch(
          '/api/sales/approvals/' + encodeURIComponent(id) + '/edit',
          { edited_payload: this.buildEditedPayload(approval) }
        );
        var updated = data && data.approval ? data.approval : null;
        if (updated) {
          for (var j = 0; j < this.approvals.length; j++) {
            if (this.approvals[j].id === id) {
              this.approvals[j] = updated;
              break;
            }
          }
          delete this.approvalDrafts[id];
        }
        this.approvalEditorId = '';
        OpenFangToast.success('Taslak guncellendi');
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Taslak guncellenemedi');
      }
    },

    async bulkApproveSelected() {
      var ids = [];
      var items = this.pendingApprovalItems;
      for (var i = 0; i < items.length; i++) {
        if (this.approvalSelections[items[i].id]) ids.push(items[i].id);
      }
      if (!ids.length) {
        OpenFangToast.error('Onaylanacak taslak secilmedi');
        return;
      }
      try {
        var data = await OpenFangAPI.post('/api/sales/approvals/bulk-approve', { ids: ids });
        var approved = data && Array.isArray(data.approved) ? data.approved.length : 0;
        var failed = data && Array.isArray(data.failed) ? data.failed.length : 0;
        this.approvalSelections = {};
        await Promise.all([this.loadApprovals(), this.loadDeliveries()]);
        if (approved > 0) {
          OpenFangToast.success(String(approved) + ' taslak onaylandi');
        }
        if (failed > 0) {
          OpenFangToast.warn(String(failed) + ' taslak gonderilemedi', 8000);
        }
      } catch (e) {
        OpenFangToast.error(e && e.message ? e.message : 'Toplu onay basarisiz');
      }
    },

    handleApprovalHotkeys(evt) {
      if (this.showOnboarding || this.activeTab !== 'approvals') return;
      var tag = document.activeElement && document.activeElement.tagName ? document.activeElement.tagName.toUpperCase() : '';
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
      if (!this.pendingApprovalItems.length) return;

      if (evt.key === 'ArrowDown') {
        evt.preventDefault();
        this.moveApprovalCursor(1);
        return;
      }
      if (evt.key === 'ArrowRight') {
        evt.preventDefault();
        var current = this.currentPendingApproval();
        if (current) this.approve(current.id);
        return;
      }
      if (evt.key === 'ArrowLeft') {
        evt.preventDefault();
        var currentReject = this.currentPendingApproval();
        if (currentReject) this.reject(currentReject.id);
        return;
      }
      if (evt.key === 'ArrowUp') {
        evt.preventDefault();
        var currentEdit = this.currentPendingApproval();
        if (currentEdit) this.startApprovalEdit(currentEdit);
      }
    },
    approvalRecipient(a) {
      if (!a || !a.payload) return '-';
      if (a.channel === 'email') return a.payload.to || '-';
      if (a.channel === 'linkedin' || a.channel === 'linkedin_assist') return a.payload.profile_url || '-';
      return '-';
    },

    approvalTitle(a) {
      if (!a || !a.payload) return 'Taslak';
      if (a.channel === 'email') return a.payload.subject || 'E-posta taslagi';
      if (a.channel === 'linkedin' || a.channel === 'linkedin_assist') return 'LinkedIn operator assist';
      return 'Taslak';
    },

    approvalBody(a) {
      if (!a || !a.payload) return '';
      if (a.channel === 'email') return a.payload.body || '';
      if (a.channel === 'linkedin' || a.channel === 'linkedin_assist') return a.payload.message || '';
      return '';
    },
    async approve(id) {
      try {
        await OpenFangAPI.post('/api/sales/approvals/' + encodeURIComponent(id) + '/approve', {});
        OpenFangToast.success('Onaylandi');
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
};
