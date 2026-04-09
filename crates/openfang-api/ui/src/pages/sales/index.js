'use strict';

import { createSalesState, salesStateMixins } from './state.js';
import { salesDataMixins } from './data.js';
import { salesJobMixins } from './jobs.js';
import { salesProspectMixins } from './prospects.js';
import { salesApprovalMixins } from './approvals.js';
import { salesOauthMixins } from './oauth.js';
import { salesFormatterMixins } from './formatters.js';

function applyMixin(target, mixin) {
  Object.defineProperties(target, Object.getOwnPropertyDescriptors(mixin));
  return target;
}

export function createSalesPage() {
  var page = createSalesState();
  var mixins = [
    salesStateMixins,
    salesDataMixins,
    salesJobMixins,
    salesProspectMixins,
    salesApprovalMixins,
    salesOauthMixins,
    salesFormatterMixins
  ];

  for (var i = 0; i < mixins.length; i++) {
    applyMixin(page, mixins[i]);
  }

  return page;
}
