/* cloud-db.js — Central Realtime Firebase Cloud Engine for YKS Akıllı Ders Planlayıcı */

const CloudDB = {
    defaultUrl: 'https://yks-planlayici-default-rtdb.europe-west1.firebasedatabase.app/yks_planner.json',
    databaseUrl: 'https://yks-planlayici-default-rtdb.europe-west1.firebasedatabase.app/yks_planner.json',
    syncStatus: 'synced', // 'syncing' | 'synced' | 'offline' | 'error'
    lastSyncTime: null,
    eventSource: null,
    pollInterval: null,
    pushDebounceTimer: null,
    isApplyingRemote: false,

    initDatabaseUrl() {
        try {
            const savedUrl = localStorage.getItem('yks_firebase_url');
            if (savedUrl && savedUrl.trim().startsWith('http')) {
                let clean = savedUrl.trim();
                if (!clean.endsWith('.json')) clean = clean.replace(/\/+$/, '') + '/yks_planner.json';
                this.databaseUrl = clean;
            } else {
                this.databaseUrl = this.defaultUrl;
            }
        } catch (e) {
            this.databaseUrl = this.defaultUrl;
        }
        return this.databaseUrl;
    },

    async initAndFetch(defaultMaster, defaultCurriculum) {
        this.initDatabaseUrl();
        this.updateHeaderBadge();

        // 1. Fetch live data directly from Firebase
        let cloudData = null;
        if (navigator.onLine) {
            try {
                this.syncStatus = 'syncing';
                this.updateHeaderBadge();
                const res = await fetch(this.databaseUrl, {
                    method: 'GET',
                    headers: { 'Accept': 'application/json' }
                });
                if (res.ok) {
                    cloudData = await res.json();
                }
            } catch (err) {
                console.warn('Firebase ilk yükleme uyarısı:', err);
            }
        }

        if (cloudData && typeof cloudData === 'object' && cloudData.activePlan && Array.isArray(cloudData.activePlan) && cloudData.activePlan.length > 0) {
            this.applyRemoteDataToApp(cloudData);
            this.lastSyncTime = new Date();
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
        } else {
            // Firebase is empty or unreachable — initialize with default master data & push to Firebase
            activePlan = JSON.parse(JSON.stringify(defaultMaster));
            appCurriculum = JSON.parse(JSON.stringify(defaultCurriculum));
            completedSessions = {};
            sessionNotes = {};
            archivedPlans = [];
            globalDailyLimit = 10;
            currentTheme = 'slate-dark';
            if (navigator.onLine) {
                await this.pushToCloud();
            }
        }

        // 2. Connect Realtime Server-Sent Events (SSE) stream for instant live syncing across all open devices
        this.connectLiveStream();

        // 3. Online/offline listeners
        window.addEventListener('online', () => {
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            this.pullFromCloud(true);
            this.connectLiveStream();
        });

        window.addEventListener('offline', () => {
            this.syncStatus = 'offline';
            this.updateHeaderBadge();
            if (this.eventSource) {
                try { this.eventSource.close(); } catch(e){}
            }
        });

        document.addEventListener('visibilitychange', () => {
            if (!document.hidden && navigator.onLine) {
                this.pullFromCloud(true);
            }
        });

        window.addEventListener('focus', () => {
            if (navigator.onLine) {
                this.pullFromCloud(true);
            }
        });

        // Periodic background poll every 15s
        if (this.pollInterval) clearInterval(this.pollInterval);
        this.pollInterval = setInterval(() => {
            if (navigator.onLine && !document.hidden && !this.isApplyingRemote) {
                this.pullFromCloud(true);
            }
        }, 15000);
    },

    connectLiveStream() {
        if (!navigator.onLine || typeof EventSource === 'undefined') return;
        try {
            if (this.eventSource) {
                this.eventSource.close();
            }

            this.eventSource = new EventSource(this.databaseUrl);

            this.eventSource.addEventListener('put', (e) => {
                if (!e.data || this.isApplyingRemote) return;
                try {
                    const parsed = JSON.parse(e.data);
                    if (parsed && typeof parsed === 'object') {
                        if (parsed.path === '/' && parsed.data && typeof parsed.data === 'object') {
                            this.handleRemoteDataUpdate(parsed.data);
                        } else if (parsed.path && parsed.path !== '/') {
                            this.pullFromCloud(true);
                        }
                    }
                } catch(err) {}
            });

            this.eventSource.addEventListener('patch', (e) => {
                if (!this.isApplyingRemote) {
                    this.pullFromCloud(true);
                }
            });

            this.eventSource.onerror = () => {
                if (this.eventSource) {
                    try { this.eventSource.close(); } catch(e){}
                    this.eventSource = null;
                }
            };
        } catch (e) {
            console.warn('Firebase SSE connection:', e);
        }
    },

    getFullAppState() {
        const cleanPlan = (typeof activePlan !== 'undefined' && Array.isArray(activePlan)) 
            ? activePlan.map(d => ({
                ...d,
                sessions: Array.isArray(d.sessions) ? d.sessions : []
            })) 
            : [];

        const catOrder = (typeof appCurriculum === 'object' && appCurriculum !== null) ? Object.keys(appCurriculum) : [];

        return {
            activePlan: cleanPlan,
            completedSessions: (typeof completedSessions === 'object' && completedSessions !== null) ? completedSessions : {},
            sessionNotes: (typeof sessionNotes === 'object' && sessionNotes !== null) ? sessionNotes : {},
            archivedPlans: (typeof archivedPlans !== 'undefined' && Array.isArray(archivedPlans)) ? archivedPlans : [],
            appCurriculum: (typeof appCurriculum === 'object' && appCurriculum !== null) ? appCurriculum : {},
            curriculumCategoryOrder: catOrder,
            globalDailyLimit: (typeof globalDailyLimit === 'number') ? globalDailyLimit : 10,
            currentTheme: (typeof currentTheme === 'string') ? currentTheme : 'slate-dark',
            activityLogs: (typeof AppDB !== 'undefined' && Array.isArray(AppDB.logsCache)) ? AppDB.logsCache.slice(0, 100) : [],
            customVideoLinks: (typeof customVideoLinks === 'object' && customVideoLinks !== null) ? customVideoLinks : {},
            llmConfig: (typeof llmConfig === 'object' && llmConfig !== null) ? llmConfig : {},
            lastUpdated: new Date().toISOString()
        };
    },

    applyRemoteDataToApp(remoteData) {
        if (!remoteData || typeof remoteData !== 'object') return false;

        if (remoteData.activePlan && Array.isArray(remoteData.activePlan) && remoteData.activePlan.length > 0) {
            remoteData.activePlan.forEach((d, idx) => {
                if (typeof d.day !== 'number') d.day = idx + 1;
                if (!d.title) d.title = `${d.day}. Gün Çalışma Planı`;
                if (!Array.isArray(d.sessions)) d.sessions = [];
            });
            activePlan = remoteData.activePlan;
            try { localStorage.setItem('yks_active_plan_v2', JSON.stringify(activePlan)); } catch(e){}
            if (typeof AppDB !== 'undefined' && AppDB.db) {
                try {
                    const tx = AppDB.db.transaction('study_plans', 'readwrite');
                    const store = tx.objectStore('study_plans');
                    store.clear();
                    activePlan.forEach(d => store.put(d));
                } catch(e){}
            }
        }

        if (remoteData.completedSessions && typeof remoteData.completedSessions === 'object') {
            completedSessions = remoteData.completedSessions;
            try { localStorage.setItem('yks_setting_completedSessions', JSON.stringify(completedSessions)); } catch(e){}
        }

        if (remoteData.sessionNotes && typeof remoteData.sessionNotes === 'object') {
            sessionNotes = remoteData.sessionNotes;
            try { localStorage.setItem('yks_setting_sessionNotes', JSON.stringify(sessionNotes)); } catch(e){}
        }

        if (remoteData.archivedPlans && Array.isArray(remoteData.archivedPlans)) {
            archivedPlans = remoteData.archivedPlans;
            try { localStorage.setItem('yks_setting_archivedPlans', JSON.stringify(archivedPlans)); } catch(e){}
        }

        if (remoteData.appCurriculum && typeof remoteData.appCurriculum === 'object' && Object.keys(remoteData.appCurriculum).length > 0) {
            const orderedCurriculum = {};
            const catOrder = Array.isArray(remoteData.curriculumCategoryOrder) ? remoteData.curriculumCategoryOrder : Object.keys(remoteData.appCurriculum);
            catOrder.forEach(k => {
                if (remoteData.appCurriculum[k]) orderedCurriculum[k] = remoteData.appCurriculum[k];
            });
            Object.keys(remoteData.appCurriculum).forEach(k => {
                if (!orderedCurriculum[k]) orderedCurriculum[k] = remoteData.appCurriculum[k];
            });
            appCurriculum = orderedCurriculum;
            try { localStorage.setItem('yks_custom_curriculum', JSON.stringify(appCurriculum)); } catch(e){}
        }

        if (typeof remoteData.globalDailyLimit === 'number' && remoteData.globalDailyLimit > 0) {
            globalDailyLimit = remoteData.globalDailyLimit;
            try { localStorage.setItem('yks_setting_globalDailyLimit', JSON.stringify(globalDailyLimit)); } catch(e){}
            const limitSel = document.getElementById('globalDailyLimitSelect');
            if (limitSel) limitSel.value = String(globalDailyLimit);
        }

        if (remoteData.customVideoLinks && typeof remoteData.customVideoLinks === 'object') {
            customVideoLinks = remoteData.customVideoLinks;
            if (typeof applyCustomLinksToPlan === 'function' && typeof activePlan !== 'undefined') {
                applyCustomLinksToPlan(activePlan);
            }
        }

        if (remoteData.currentTheme && typeof remoteData.currentTheme === 'string') {
            currentTheme = remoteData.currentTheme;
            if (typeof setTheme === 'function') setTheme(currentTheme);
        }

        if (remoteData.llmConfig && typeof remoteData.llmConfig === 'object') {
            llmConfig = remoteData.llmConfig;
            if (typeof loadLLMSettingsToUI === 'function') loadLLMSettingsToUI();
        }

        if (remoteData.activityLogs && Array.isArray(remoteData.activityLogs) && remoteData.activityLogs.length > 0) {
            if (typeof AppDB !== 'undefined') {
                AppDB.logsCache = remoteData.activityLogs;
                try { localStorage.setItem('yks_activity_logs', JSON.stringify(AppDB.logsCache.slice(0, 50))); } catch(e){}
            }
        }

        return true;
    },

    handleRemoteDataUpdate(remoteData) {
        if (!remoteData || typeof remoteData !== 'object') return;
        if (this.isApplyingRemote) return;

        try {
            this.isApplyingRemote = true;
            this.applyRemoteDataToApp(remoteData);

            if (typeof ensurePlanIntegrity === 'function' && typeof activePlan !== 'undefined') {
                ensurePlanIntegrity(activePlan);
            }
            if (typeof updatePlanHeadersAndTitles === 'function') updatePlanHeadersAndTitles();
            if (typeof updateHeaderPlanInfo === 'function') updateHeaderPlanInfo();
            if (typeof renderDaysTabBar === 'function') renderDaysTabBar();
            if (typeof renderActiveDay === 'function') renderActiveDay();
            if (typeof renderFullTable === 'function') renderFullTable();
            if (typeof renderCurriculumLibrary === 'function') renderCurriculumLibrary();
            if (typeof updateOverallProgress === 'function') updateOverallProgress();
            if (typeof generateAICoachInsights === 'function') generateAICoachInsights();
            if (typeof updateDbStatsBadge === 'function') updateDbStatsBadge();
            if (typeof updateArchiveCountBadge === 'function') updateArchiveCountBadge();
            if (typeof refreshDbLogsUI === 'function' && document.getElementById('databaseModal') && !document.getElementById('databaseModal').classList.contains('hidden')) {
                refreshDbLogsUI();
            }
        } finally {
            this.isApplyingRemote = false;
        }
    },

    schedulePush(delayMs = 250) {
        if (this.isApplyingRemote) return;

        if (!navigator.onLine) {
            this.syncStatus = 'offline';
            this.updateHeaderBadge();
            return;
        }

        this.syncStatus = 'syncing';
        this.updateHeaderBadge();

        if (this.pushDebounceTimer) {
            clearTimeout(this.pushDebounceTimer);
        }

        this.pushDebounceTimer = setTimeout(() => {
            this.pushToCloud();
        }, delayMs);
    },

    async pushToCloud() {
        if (!navigator.onLine) {
            this.syncStatus = 'offline';
            this.updateHeaderBadge();
            return false;
        }

        this.syncStatus = 'syncing';
        this.updateHeaderBadge();

        try {
            const now = new Date();
            const payload = this.getFullAppState();
            payload.lastUpdated = now.toISOString();

            const res = await fetch(this.databaseUrl, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            if (!res.ok) {
                throw new Error(`HTTP ${res.status}`);
            }

            this.lastSyncTime = now;
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            this.updateModalCloudStatus();
            return true;
        } catch (err) {
            console.warn('Firebase bulut yazma uyarısı:', err);
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            return false;
        }
    },

    async pullFromCloud(silent = false) {
        if (!navigator.onLine) {
            this.syncStatus = 'offline';
            this.updateHeaderBadge();
            return null;
        }

        if (!silent) {
            this.syncStatus = 'syncing';
            this.updateHeaderBadge();
        }

        try {
            const res = await fetch(this.databaseUrl, {
                method: 'GET',
                headers: { 'Accept': 'application/json' }
            });

            if (!res.ok) throw new Error(`HTTP ${res.status}`);

            const cloudData = await res.json();
            if (!cloudData || typeof cloudData !== 'object') {
                this.pushToCloud();
                this.syncStatus = 'synced';
                this.updateHeaderBadge();
                return null;
            }

            this.handleRemoteDataUpdate(cloudData);
            this.lastSyncTime = new Date();
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            this.updateModalCloudStatus();
            return cloudData;
        } catch (err) {
            console.warn('Firebase bulut okuma:', err);
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            return null;
        }
    },

    setDatabaseUrl(newUrl) {
        if (!newUrl || !newUrl.trim()) {
            this.databaseUrl = this.defaultUrl;
            try { localStorage.removeItem('yks_firebase_url'); } catch(e){}
        } else {
            let clean = newUrl.trim();
            if (!clean.endsWith('.json')) {
                clean = clean.replace(/\/+$/, '') + '/yks_planner.json';
            }
            this.databaseUrl = clean;
            try { localStorage.setItem('yks_firebase_url', clean); } catch(e){}
        }
        this.connectLiveStream();
        this.pullFromCloud(false);
        this.updateModalCloudStatus();
        this.updateHeaderBadge();
    },

    updateHeaderBadge() {
        let badge = document.getElementById('cloudSyncHeaderBadge');
        if (!badge) badge = document.getElementById('dbStatusHeaderBtn');
        if (!badge) return;

        let icon = '🟢';
        let fullText = 'Firebase Canlı Veritabanı';
        let shortText = 'Bulut Aktif';
        let colorClass = 'text-emerald-400 bg-emerald-500/10 border-emerald-500/30';

        if (!navigator.onLine) {
            icon = '📴';
            fullText = 'Çevrimdışı';
            shortText = 'Çevrimdışı';
            colorClass = 'text-slate-400 bg-slate-500/10 border-slate-500/30';
        } else if (this.syncStatus === 'syncing') {
            icon = '🔄';
            fullText = 'Firebase Güncelleniyor...';
            shortText = 'Kaydediliyor';
            colorClass = 'text-amber-400 bg-amber-500/10 border-amber-500/30';
        }

        badge.className = `px-3 py-1.5 text-xs font-semibold rounded-lg border flex items-center gap-1.5 transition-all shadow-sm ${colorClass}`;
        badge.innerHTML = `
            <span class="inline-block w-2 h-2 rounded-full ${this.syncStatus === 'syncing' ? 'bg-amber-400 animate-spin' : (navigator.onLine ? 'bg-emerald-400 animate-pulse' : 'bg-slate-400')}"></span>
            <span class="hidden sm:inline">${icon} ${fullText}</span>
            <span class="sm:hidden">${icon} ${shortText}</span>
        `;
    },

    updateModalCloudStatus() {
        const statusEl = document.getElementById('cloudModalStatusText');
        const timeEl = document.getElementById('cloudModalLastSyncTime');
        const sseEl = document.getElementById('cloudModalSseText');
        const urlInput = document.getElementById('firebaseDbUrlInput');

        if (statusEl) {
            if (!navigator.onLine) {
                statusEl.innerHTML = '<span class="text-slate-400 font-bold">📴 Çevrimdışı</span>';
            } else if (this.syncStatus === 'syncing') {
                statusEl.innerHTML = '<span class="text-amber-400 font-bold">🔄 Firebase Eşitleniyor...</span>';
            } else {
                statusEl.innerHTML = '<span class="text-emerald-400 font-bold">🟢 Firebase Realtime DB Canlı Bağlı</span>';
            }
        }

        if (timeEl && this.lastSyncTime) {
            timeEl.innerText = this.lastSyncTime.toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) + ' (' + this.lastSyncTime.toLocaleDateString('tr-TR') + ')';
        }

        if (sseEl) {
            sseEl.innerHTML = this.eventSource ? '<span class="text-emerald-400 font-bold">🟢 Canlı Akış Aktif (Server-Sent Events)</span>' : '<span class="text-indigo-400 font-bold">🔄 Polling Devrede</span>';
        }

        if (urlInput) {
            urlInput.value = this.databaseUrl;
        }
    }
};

if (typeof window !== 'undefined') window.CloudDB = CloudDB;
if (typeof global !== 'undefined') global.CloudDB = CloudDB;

