'use strict';

import { PulsivoSalesmanAPI, PulsivoSalesmanToast } from './core/api.js';
import { createApp } from './core/app-shell.js';
import { createSalesPage } from './pages/sales/index.js';

window.PulsivoSalesmanAPI = PulsivoSalesmanAPI;
window.PulsivoSalesmanToast = PulsivoSalesmanToast;
window.app = createApp;
window.salesPage = createSalesPage;
