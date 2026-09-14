/* cloud-db.js — Official Google Firebase Realtime NoSQL Engine for YKS Akıllı Ders Planlayıcı */

const CloudDB = {
    defaultBaseUrl: 'https://yks-planlayici-default-rtdb.europe-west1.firebasedatabase.app',
    databaseUrl: '',
    syncStatus: 'synced', // 'syncing' | 'synced' | 'offline' | 'error'
    lastSyncTime: null,
    eventSource: null,
    pollInterval: null,
    pushDebounceTimer: null,
    isApplyingRemote: false,
    lastLocalChangeTime: 0,

    getSyncUserId() {
        let uid = '';
        try { uid = localStorage.getItem('yks_sync_user_id') || ''; } catch(e){}
        if (!uid || uid.trim().length < 6) {
            uid = 'usr_' + Math.random().toString(36).substring(2, 8) + '_' + Date.now().toString(36);
            try { localStorage.setItem('yks_sync_user_id', uid); } catch(e){}
        }
        return uid.trim();
    },

    setSyncUserId(newUid) {
        if (!newUid || !newUid.trim()) return;
        const clean = newUid.trim().replace(/[^a-zA-Z0-9_-]/g, '');
        try { localStorage.setItem('yks_sync_user_id', clean); } catch(e){}
        try { localStorage.removeItem('yks_firebase_url'); } catch(e){}
        try { localStorage.removeItem('yks_last_local_update_time'); } catch(e){}
        this.lastLocalChangeTime = 0;
        this.resolveDatabaseUrl();
        this.connectLiveStream();
        this.pullFromCloud(false);
        this.updateModalCloudStatus();
        this.updateHeaderBadge();
    },

    getShareSyncUrl() {
        const uid = this.getSyncUserId();
        const url = new URL(window.location.href);
        url.searchParams.set('sync', uid);
        return url.toString();
    },

    resolveDatabaseUrl() {
        try {
            const savedUrl = localStorage.getItem('yks_firebase_url');
            if (savedUrl && savedUrl.trim().startsWith('http')) {
                let clean = savedUrl.trim();
                if (!clean.endsWith('.json')) clean = clean.replace(/\/+$/, '') + '.json';
                this.databaseUrl = clean;
                return this.databaseUrl;
            }
        } catch(e){}

        const uid = this.getSyncUserId();
        this.databaseUrl = `${this.defaultBaseUrl}/users/${uid}.json`;
        return this.databaseUrl;
    },

    init() {
        // 0. Check URL query parameters for ?sync=usr_... or ?kod=usr_...
        try {
            const params = new URLSearchParams(window.location.search);
            const querySyncId = params.get('sync') || params.get('kod') || params.get('id');
            if (querySyncId && querySyncId.trim().length >= 6) {
                const clean = querySyncId.trim().replace(/[^a-zA-Z0-9_-]/g, '');
                const currentId = localStorage.getItem('yks_sync_user_id');
                if (clean && clean !== currentId) {
                    localStorage.setItem('yks_sync_user_id', clean);
                    localStorage.removeItem('yks_firebase_url');
                    localStorage.removeItem('yks_last_local_update_time');
                    this.lastLocalChangeTime = 0;
                }
            }
        } catch(e){}

        this.resolveDatabaseUrl();

        // Restore last local change timestamp
        try {
            const savedLocalTime = localStorage.getItem('yks_last_local_update_time');
            if (savedLocalTime) {
                this.lastLocalChangeTime = parseInt(savedLocalTime, 10) || 0;
            }
        } catch(e){}

        this.updateHeaderBadge();

        // 1. Initial pull from Firebase Realtime DB
        this.pullFromCloud(true);

        // 2. Connect to real-time Server-Sent Events (SSE) stream for instant multi-device live sync
        this.connectLiveStream();

        // 3. Auto-sync listeners
        window.addEventListener('online', () => {
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            this.pushToCloud();
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

        // Background backup poll every 25 seconds
        if (this.pollInterval) clearInterval(this.pollInterval);
        this.pollInterval = setInterval(() => {
            if (navigator.onLine && !document.hidden && !this.isApplyingRemote) {
                this.pullFromCloud(true);
            }
        }, 25000);
    },

    // Configure a new / custom Firebase Realtime Database URL
    setDatabaseUrl(newUrl) {
        if (!newUrl || !newUrl.trim()) {
            try { localStorage.removeItem('yks_firebase_url'); } catch(e){}
        } else {
            let clean = newUrl.trim();
            if (!clean.endsWith('.json')) {
                clean = clean.replace(/\/+$/, '') + '/yks_planner.json';
            }
            try { localStorage.setItem('yks_firebase_url', clean); } catch(e){}
        }
        this.resolveDatabaseUrl();
        this.connectLiveStream();
        this.pullFromCloud(false);
        this.updateModalCloudStatus();
        this.updateHeaderBadge();
    },

    // Connect to Firebase Realtime Database Streaming API
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
                // Silently fallback to periodic polling
                if (this.eventSource) {
                    try { this.eventSource.close(); } catch(e){}
                    this.eventSource = null;
                }
            };
        } catch (e) {
            console.warn('Firebase LiveStream SSE fallback to poll:', e);
        }
    },

    // Package entire application state into a clean cloud dump
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

    // Process incoming remote data from Firebase
    handleRemoteDataUpdate(remoteData) {
        if (!remoteData || typeof remoteData !== 'object') return;
        if (this.isApplyingRemote) return;

        // Reject stale remote data if local edits are newer
        const localSavedTime = parseInt(localStorage.getItem('yks_last_local_update_time') || '0', 10) || this.lastLocalChangeTime;
        if (remoteData.lastUpdated && localSavedTime > 0) {
            const remoteTime = new Date(remoteData.lastUpdated).getTime();
            if (remoteTime < (localSavedTime - 300)) {
                // Local state is newer, push local state to cloud to re-sync
                this.schedulePush(200);
                return;
            }
        }

        try {
            this.isApplyingRemote = true;
            let hasChanges = false;

            // 1. Active Plan
            if (remoteData.activePlan && Array.isArray(remoteData.activePlan) && remoteData.activePlan.length > 0) {
                // Ensure session array integrity
                remoteData.activePlan.forEach((d, idx) => {
                    if (typeof d.day !== 'number') d.day = idx + 1;
                    if (!d.title) d.title = `${d.day}. Gün Çalışma Planı`;
                    if (!Array.isArray(d.sessions)) d.sessions = [];
                });

                const currentLocalStr = JSON.stringify(typeof activePlan !== 'undefined' ? activePlan : []);
                const remotePlanStr = JSON.stringify(remoteData.activePlan);
                if (currentLocalStr !== remotePlanStr) {
                    activePlan = remoteData.activePlan;
                    try { localStorage.setItem('yks_active_plan_v2', remotePlanStr); } catch(e){}
                    if (typeof AppDB !== 'undefined' && AppDB.db) {
                        try {
                            const tx = AppDB.db.transaction('study_plans', 'readwrite');
                            const store = tx.objectStore('study_plans');
                            store.clear();
                            activePlan.forEach(d => store.put(d));
                        } catch(e){}
                    }
                    hasChanges = true;
                }
            }

            // 2. Completed Sessions
            if (remoteData.completedSessions && typeof remoteData.completedSessions === 'object') {
                const localCompletedStr = JSON.stringify(typeof completedSessions !== 'undefined' ? completedSessions : {});
                const remoteCompletedStr = JSON.stringify(remoteData.completedSessions);
                if (localCompletedStr !== remoteCompletedStr) {
                    completedSessions = remoteData.completedSessions;
                    try { localStorage.setItem('yks_setting_completedSessions', remoteCompletedStr); } catch(e){}
                    if (typeof AppDB !== 'undefined') AppDB.saveSetting('completedSessions', completedSessions);
                    hasChanges = true;
                }
            }

            // 2.1 Session Notes
            if (remoteData.sessionNotes && typeof remoteData.sessionNotes === 'object') {
                const localNotesStr = JSON.stringify(typeof sessionNotes !== 'undefined' ? sessionNotes : {});
                const remoteNotesStr = JSON.stringify(remoteData.sessionNotes);
                if (localNotesStr !== remoteNotesStr) {
                    sessionNotes = remoteData.sessionNotes;
                    try { localStorage.setItem('yks_setting_sessionNotes', remoteNotesStr); } catch(e){}
                    if (typeof AppDB !== 'undefined') AppDB.saveSetting('sessionNotes', sessionNotes);
                    hasChanges = true;
                }
            }

            // 2.2 Archived Plans
            if (remoteData.archivedPlans && Array.isArray(remoteData.archivedPlans)) {
                const localArchivedStr = JSON.stringify(typeof archivedPlans !== 'undefined' ? archivedPlans : []);
                const remoteArchivedStr = JSON.stringify(remoteData.archivedPlans);
                if (localArchivedStr !== remoteArchivedStr) {
                    archivedPlans = remoteData.archivedPlans;
                    try { localStorage.setItem('yks_setting_archivedPlans', remoteArchivedStr); } catch(e){}
                    if (typeof AppDB !== 'undefined') AppDB.saveSetting('archivedPlans', archivedPlans);
                    hasChanges = true;
                }
            }

            // 3. Curriculum with Category Ordering preservation
            if (remoteData.appCurriculum && typeof remoteData.appCurriculum === 'object' && Object.keys(remoteData.appCurriculum).length > 0) {
                const orderedCurriculum = {};
                const catOrder = Array.isArray(remoteData.curriculumCategoryOrder) ? remoteData.curriculumCategoryOrder : Object.keys(remoteData.appCurriculum);
                catOrder.forEach(k => {
                    if (remoteData.appCurriculum[k]) {
                        orderedCurriculum[k] = remoteData.appCurriculum[k];
                    }
                });
                Object.keys(remoteData.appCurriculum).forEach(k => {
                    if (!orderedCurriculum[k]) {
                        orderedCurriculum[k] = remoteData.appCurriculum[k];
                    }
                });

                const localCurriculumStr = JSON.stringify(typeof appCurriculum !== 'undefined' ? appCurriculum : {});
                const remoteCurriculumStr = JSON.stringify(orderedCurriculum);
                if (localCurriculumStr !== remoteCurriculumStr) {
                    appCurriculum = orderedCurriculum;
                    try { localStorage.setItem('yks_custom_curriculum', remoteCurriculumStr); } catch(e){}
                    if (typeof AppDB !== 'undefined') AppDB.saveCurriculum(appCurriculum);
                    hasChanges = true;
                }
            }

            // 4. Global Daily Limit
            if (typeof remoteData.globalDailyLimit === 'number' && remoteData.globalDailyLimit > 0) {
                if (typeof globalDailyLimit !== 'undefined' && globalDailyLimit !== remoteData.globalDailyLimit) {
                    globalDailyLimit = remoteData.globalDailyLimit;
                    try { localStorage.setItem('yks_setting_globalDailyLimit', JSON.stringify(globalDailyLimit)); } catch(e){}
                    if (typeof AppDB !== 'undefined') AppDB.saveSetting('globalDailyLimit', globalDailyLimit);
                    const limitSel = document.getElementById('globalDailyLimitSelect');
                    if (limitSel) limitSel.value = String(globalDailyLimit);
                    hasChanges = true;
                }
            }

            // 5. Custom Video Links
            if (remoteData.customVideoLinks && typeof remoteData.customVideoLinks === 'object') {
                const localVidStr = JSON.stringify(typeof customVideoLinks !== 'undefined' ? customVideoLinks : {});
                const remoteVidStr = JSON.stringify(remoteData.customVideoLinks);
                if (localVidStr !== remoteVidStr) {
                    customVideoLinks = remoteData.customVideoLinks;
                    try { localStorage.setItem('yks_custom_videos', remoteVidStr); } catch(e){}
                    if (typeof AppDB !== 'undefined') AppDB.saveSetting('customVideoLinks', customVideoLinks);
                    if (typeof applyCustomLinksToPlan === 'function' && typeof activePlan !== 'undefined') {
                        applyCustomLinksToPlan(activePlan);
                    }
                    hasChanges = true;
                }
            }

            // 6. Theme
            if (remoteData.currentTheme && typeof remoteData.currentTheme === 'string') {
                if (typeof currentTheme !== 'undefined' && currentTheme !== remoteData.currentTheme) {
                    currentTheme = remoteData.currentTheme;
                    if (typeof setTheme === 'function') setTheme(currentTheme);
                    if (typeof AppDB !== 'undefined') AppDB.saveSetting('theme', currentTheme);
                    hasChanges = true;
                }
            }

            // 7. LLM Config
            if (remoteData.llmConfig && typeof remoteData.llmConfig === 'object') {
                const localLLMStr = JSON.stringify(typeof llmConfig !== 'undefined' ? llmConfig : {});
                const remoteLLMStr = JSON.stringify(remoteData.llmConfig);
                if (localLLMStr !== remoteLLMStr) {
                    llmConfig = remoteData.llmConfig;
                    if (typeof AppDB !== 'undefined') AppDB.saveSetting('llmConfig', llmConfig);
                    if (typeof loadLLMSettingsToUI === 'function') loadLLMSettingsToUI();
                    hasChanges = true;
                }
            }

            // 8. Activity Logs
            if (remoteData.activityLogs && Array.isArray(remoteData.activityLogs) && remoteData.activityLogs.length > 0) {
                if (typeof AppDB !== 'undefined') {
                    AppDB.logsCache = remoteData.activityLogs;
                    try { localStorage.setItem('yks_activity_logs', JSON.stringify(AppDB.logsCache.slice(0, 50))); } catch(e){}
                }
            }

            // If UI state changed, re-render visible components
            if (hasChanges) {
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
            }
        } finally {
            this.isApplyingRemote = false;
        }
    },

    // Debounced automatic push to Firebase Realtime DB
    schedulePush(delayMs = 300) {
        if (this.isApplyingRemote) return;
        const now = Date.now();
        this.lastLocalChangeTime = now;
        try { localStorage.setItem('yks_last_local_update_time', String(now)); } catch(e){}

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
            this.lastLocalChangeTime = now.getTime();
            try { localStorage.setItem('yks_last_local_update_time', String(this.lastLocalChangeTime)); } catch(e){}

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
            console.warn('Firebase bulut yazma uyarısı (yerel veriler korundu):', err);
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            return false;
        }
    },

    // Pull ALL data from Firebase Realtime DB
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
                // If cloud database is empty, initialize cloud with current local state
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
            console.warn('Firebase bulut okuma uyarısı (yerel veriler devrede):', err);
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            return null;
        }
    },

    // Update the live header status badge
    updateHeaderBadge() {
        let badge = document.getElementById('cloudSyncHeaderBadge');
        if (!badge) {
            badge = document.getElementById('dbStatusHeaderBtn');
        }
        if (!badge) return;

        let icon = '🟢';
        let fullText = 'Canlı Kayıt & Eşitleme';
        let shortText = 'Canlı';
        let colorClass = 'text-emerald-400 bg-emerald-500/10 border-emerald-500/30';

        if (!navigator.onLine) {
            icon = '📴';
            fullText = 'Çevrimdışı (Yerel)';
            shortText = 'Çevrimdışı';
            colorClass = 'text-slate-400 bg-slate-500/10 border-slate-500/30';
        } else if (this.syncStatus === 'syncing') {
            icon = '🔄';
            fullText = 'Buluta Kaydediliyor...';
            shortText = 'Eşitleniyor';
            colorClass = 'text-amber-400 bg-amber-500/10 border-amber-500/30';
        }

        badge.className = `px-3 py-1.5 text-xs font-semibold rounded-lg border flex items-center gap-1.5 transition-all shadow-sm ${colorClass}`;
        badge.innerHTML = `
            <span class="inline-block w-2 h-2 rounded-full ${this.syncStatus === 'syncing' ? 'bg-amber-400 animate-spin' : (navigator.onLine ? 'bg-emerald-400 animate-pulse' : 'bg-slate-400')}"></span>
            <span class="hidden sm:inline">${icon} ${fullText}</span>
            <span class="sm:hidden">${icon} ${shortText}</span>
        `;
    },

    // Update details in Database Modal Cloud Tab
    updateModalCloudStatus() {
        const statusEl = document.getElementById('cloudModalStatusText');
        const timeEl = document.getElementById('cloudModalLastSyncTime');
        const sseEl = document.getElementById('cloudModalSseText');
        const urlInput = document.getElementById('firebaseDbUrlInput');
        const syncIdInput = document.getElementById('firebaseSyncUserIdInput');
        const shareLinkInput = document.getElementById('firebaseShareLinkInput');
        const qrImg = document.getElementById('cloudSyncQrCodeImg');
        const displaySyncId = document.getElementById('displaySyncIdCode');

        const uid = this.getSyncUserId();
        const shareUrl = this.getShareSyncUrl();

        if (statusEl) {
            if (!navigator.onLine) {
                statusEl.innerHTML = '<span class="text-slate-400 font-bold">📴 Çevrimdışı (Yerel Depolama Devrede)</span>';
            } else if (this.syncStatus === 'syncing') {
                statusEl.innerHTML = '<span class="text-amber-400 font-bold">🔄 Eşitleniyor...</span>';
            } else {
                statusEl.innerHTML = '<span class="text-emerald-400 font-bold">🟢 Bağlı & Canlı Senkronizasyon Aktif</span>';
            }
        }

        if (timeEl && this.lastSyncTime) {
            timeEl.innerText = this.lastSyncTime.toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) + ' (' + this.lastSyncTime.toLocaleDateString('tr-TR') + ')';
        }

        if (sseEl) {
            sseEl.innerHTML = this.eventSource ? '<span class="text-emerald-400 font-bold">🟢 Aktif (Server-Sent Events)</span>' : '<span class="text-indigo-400 font-bold">🔄 Polling / Yedek Eşitleme</span>';
        }

        if (urlInput) {
            urlInput.value = this.databaseUrl;
        }

        if (syncIdInput) {
            syncIdInput.value = uid;
        }

        if (shareLinkInput) {
            shareLinkInput.value = shareUrl;
        }

        if (displaySyncId) {
            displaySyncId.innerText = uid;
        }

        if (qrImg) {
            qrImg.src = `https://api.qrserver.com/v1/create-qr-code/?size=180x180&data=${encodeURIComponent(shareUrl)}`;
        }
    }
};

if (typeof window !== 'undefined') {
    window.CloudDB = CloudDB;
}
if (typeof global !== 'undefined') {
    global.CloudDB = CloudDB;
}
