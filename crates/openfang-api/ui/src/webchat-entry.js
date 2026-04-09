'use strict';

import { OpenFangAPI, OpenFangToast } from './core/api.js';
import { createApp } from './core/app-shell.js';
import { createSalesPage } from './pages/sales/index.js';

window.OpenFangAPI = OpenFangAPI;
window.OpenFangToast = OpenFangToast;
window.app = createApp;
window.salesPage = createSalesPage;
