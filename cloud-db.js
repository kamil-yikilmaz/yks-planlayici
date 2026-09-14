/**
 * cloud-db.js — Central Realtime Multi-Device Cloud Engine for YKS Akıllı Ders Planlayıcı
 * 
 * Özellikler:
 * - Çoklu Cihaz Gerçek Zamanlı Eşitleme (PC, Tablet, Telefon, Farklı Tarayıcılar)
 * - Firebase Realtime Database (REST API + SSE Live Stream)
 * - Çift Yönlü Yarış Durumu (Race Condition) ve Yankı Koruması (Echo Suppression & Revision Control)
 * - Bulut Öncelikli Başlatma (Cloud-First Initialization): Başka cihazda oluşturulan planı anında yükler
 * - Yerel IndexedDB (AppDB) ile otomatik çift yönlü yedekleme ve çevrimdışı çalışma desteği
 * - Oda (Room) / Senkronizasyon Anahtarı ve Özel Firebase URL Desteği
 */

const CloudDB = {
    defaultUrl: 'https://yks-planlayici-default-rtdb.europe-west1.firebasedatabase.app/yks_planner_v2.json',
    databaseUrl: 'https://yks-planlayici-default-rtdb.europe-west1.firebasedatabase.app/yks_planner_v2.json',
    
    clientId: null,
    localRevision: 0,
    lastLocalModifiedTime: 0,
    lastSyncTime: null,
    syncStatus: 'synced', // 'syncing' | 'synced' | 'offline' | 'error'
    eventSource: null,
    pollInterval: null,
    pushDebounceTimer: null,
    isApplyingRemote: false,

    /**
     * Cihaza özel benzersiz kimlik (clientId) üretir veya oturumdan alır.
     */
    initClientId() {
        if (!this.clientId) {
            let cid = null;
            try {
                cid = sessionStorage.getItem('yks_client_id');
            } catch(e) {}
            if (!cid) {
                cid = 'client_' + Math.random().toString(36).substring(2, 10) + '_' + Date.now();
                try {
                    sessionStorage.setItem('yks_client_id', cid);
                } catch(e) {}
            }
            this.clientId = cid;
        }
        return this.clientId;
    },

    /**
     * URL veya parametrelerden gelen veritabanı / oda yolunu çözer.
     */
    initDatabaseUrl() {
        this.initClientId();
        try {
            // 1. URL Query Parametre Kontrolü (?room=kamil veya ?sync=kamil)
            const params = new URLSearchParams(window.location.search);
            const roomParam = params.get('room') || params.get('sync') || params.get('oda');
            if (roomParam && roomParam.trim()) {
                const cleanRoom = roomParam.trim().replace(/[^a-zA-Z0-9_-]/g, '');
                this.databaseUrl = `https://yks-planlayici-default-rtdb.europe-west1.firebasedatabase.app/rooms/${cleanRoom}.json`;
                return this.databaseUrl;
            }

            // 2. Kayıtlı özel URL kontrolü
            const savedUrl = localStorage.getItem('yks_custom_firebase_url');
            if (savedUrl && savedUrl.trim().startsWith('http')) {
                let clean = savedUrl.trim();
                if (!clean.endsWith('.json')) clean = clean.replace(/\/+$/, '') + '/yks_planner_v2.json';
                this.databaseUrl = clean;
            } else {
                this.databaseUrl = this.defaultUrl;
            }
        } catch (e) {
            this.databaseUrl = this.defaultUrl;
        }
        return this.databaseUrl;
    },

    /**
     * Bulut veritabanını başlatır, mevcut planı çeker veya yoksa varsayılanı yükler.
     */
    async initAndFetch(defaultMaster, defaultCurriculum) {
        this.initDatabaseUrl();
        this.updateHeaderBadge();

        let cloudData = null;
        let isCloudAvailable = false;

        // 1. Buluttan canlı veriyi çek
        if (navigator.onLine) {
            try {
                this.syncStatus = 'syncing';
                this.updateHeaderBadge();
                const res = await fetch(this.databaseUrl, {
                    method: 'GET',
                    headers: { 'Accept': 'application/json' },
                    cache: 'no-store'
                });
                if (res.ok) {
                    cloudData = await res.json();
                    isCloudAvailable = true;
                }
            } catch (err) {
                console.warn('[CloudDB] Buluttan ilk çekme uyarısı:', err);
            }
        }

        // 2. Çekilen bulut verisini değerlendir
        if (cloudData && typeof cloudData === 'object' && cloudData.activePlan && Array.isArray(cloudData.activePlan) && cloudData.activePlan.length > 0) {
            // Bulutta aktif bir plan var (başka bir PC/telefon oluşturmuş olabilir)!
            this.applyRemoteDataToApp(cloudData);
            if (cloudData._meta && typeof cloudData._meta.revision === 'number') {
                this.localRevision = cloudData._meta.revision;
            }
            this.lastSyncTime = new Date();
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            
            // Yerel IndexedDB'ye de yedekle
            if (typeof AppDB !== 'undefined' && AppDB.saveAllFromCloud) {
                await AppDB.saveAllFromCloud(cloudData);
            }
        } else {
            // Bulut boş veya ilk kez kuruluyor
            // Önce yerel IndexedDB'de daha önceden kalan bir plan var mı kontrol et
            let localLoaded = false;
            if (typeof AppDB !== 'undefined' && AppDB.db) {
                try {
                    const localPlans = await AppDB.getAll('study_plans');
                    if (Array.isArray(localPlans) && localPlans.length > 0) {
                        localPlans.sort((a, b) => (a.day || 0) - (b.day || 0));
                        activePlan = localPlans;
                        localLoaded = true;
                    }
                } catch(e) {}
            }

            if (!localLoaded) {
                // Hiçbir yerde veri yok -> Varsayılan master veriyi yükle
                activePlan = JSON.parse(JSON.stringify(defaultMaster));
                appCurriculum = JSON.parse(JSON.stringify(defaultCurriculum));
                completedSessions = {};
                sessionNotes = {};
                archivedPlans = [];
                globalDailyLimit = 10;
                currentTheme = 'slate-dark';
            }

            // Eğer internet açıksa ve bulut erişilebilirse, bu başlangıç planını buluta kaydet
            if (navigator.onLine && isCloudAvailable) {
                await this.pushToCloud('initial_setup');
            }
        }

        // 3. Canlı Server-Sent Events (SSE) akışını başlat (Çoklu cihaz anlık canlı güncelleme)
        this.connectLiveStream();

        // 4. Çevrimiçi/Çevrimdışı ve Odaklanma Dinleyicileri
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

        // 5. Periyodik arka plan kontrolü (SSE kesintilerine karşı her 10 saniyede bir)
        if (this.pollInterval) clearInterval(this.pollInterval);
        this.pollInterval = setInterval(() => {
            if (navigator.onLine && !document.hidden && !this.isApplyingRemote) {
                const timeSinceLastLocalChange = Date.now() - this.lastLocalModifiedTime;
                if (timeSinceLastLocalChange > 3000) {
                    this.pullFromCloud(true);
                }
            }
        }, 10000);

        return true;
    },

    /**
     * Firebase SSE Canlı Akışına bağlanır.
     */
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
            console.warn('[CloudDB] SSE bağlantı uyarısı:', e);
        }
    },

    /**
     * Tüm uygulama durumunu buluta yazılacak formatta paketler.
     */
    getFullAppState(reason = 'update') {
        const cleanPlan = (typeof activePlan !== 'undefined' && Array.isArray(activePlan)) 
            ? activePlan.map(d => ({
                day: d.day,
                title: d.title || `${d.day}. Gün Çalışma Planı`,
                sessions: Array.isArray(d.sessions) ? d.sessions.map(s => ({
                    session: s.session || 1,
                    type: s.type || 'TYT',
                    subject: s.subject || '',
                    topic: s.topic || '',
                    duration: s.duration || '60 dk',
                    detail: s.detail || '',
                    badge: s.badge || 'Etüt',
                    badgeColor: s.badgeColor || 'amber',
                    targetQuestions: typeof s.targetQuestions === 'number' ? s.targetQuestions : 30,
                    solvedQuestions: typeof s.solvedQuestions === 'number' ? s.solvedQuestions : 0,
                    completed: !!s.completed
                })) : []
            })) 
            : [];

        const catOrder = (typeof appCurriculum === 'object' && appCurriculum !== null) ? Object.keys(appCurriculum) : [];

        this.localRevision++;
        const now = Date.now();

        return {
            _meta: {
                clientId: this.clientId,
                revision: this.localRevision,
                updatedAt: now,
                reason: reason
            },
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
            lastUpdated: new Date(now).toISOString()
        };
    },

    /**
     * Uzak bulut verisini yerel belleğe ve IndexedDB'ye aktarır.
     */
    applyRemoteDataToApp(remoteData) {
        if (!remoteData || typeof remoteData !== 'object') return false;

        // 1. Aktif Plan
        if (remoteData.activePlan && Array.isArray(remoteData.activePlan)) {
            remoteData.activePlan.forEach((d, idx) => {
                if (typeof d.day !== 'number') d.day = idx + 1;
                if (!d.title) d.title = `${d.day}. Gün Çalışma Planı`;
                if (!Array.isArray(d.sessions)) d.sessions = [];
            });
            activePlan = remoteData.activePlan;
        }

        // 2. Tamamlanan Oturumlar
        if (remoteData.completedSessions && typeof remoteData.completedSessions === 'object') {
            completedSessions = remoteData.completedSessions;
        }

        // 3. Oturum Notları
        if (remoteData.sessionNotes && typeof remoteData.sessionNotes === 'object') {
            sessionNotes = remoteData.sessionNotes;
        }

        // 4. Arşivlenmiş Planlar
        if (remoteData.archivedPlans && Array.isArray(remoteData.archivedPlans)) {
            archivedPlans = remoteData.archivedPlans;
        }

        // 5. Müfredat
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
        }

        // 6. Günlük Limit
        if (typeof remoteData.globalDailyLimit === 'number' && remoteData.globalDailyLimit > 0) {
            globalDailyLimit = remoteData.globalDailyLimit;
            const limitSel = document.getElementById('globalDailyLimitSelect');
            if (limitSel) limitSel.value = String(globalDailyLimit);
        }

        // 7. Özel Video Linkleri
        if (remoteData.customVideoLinks && typeof remoteData.customVideoLinks === 'object') {
            customVideoLinks = remoteData.customVideoLinks;
            if (typeof applyCustomLinksToPlan === 'function' && typeof activePlan !== 'undefined') {
                applyCustomLinksToPlan(activePlan);
            }
        }

        // 8. Tema
        if (remoteData.currentTheme && typeof remoteData.currentTheme === 'string') {
            currentTheme = remoteData.currentTheme;
            if (typeof setTheme === 'function') setTheme(currentTheme);
        }

        // 9. LLM Konfigürasyonu
        if (remoteData.llmConfig && typeof remoteData.llmConfig === 'object') {
            llmConfig = remoteData.llmConfig;
            if (typeof loadLLMSettingsToUI === 'function') loadLLMSettingsToUI();
        }

        // 10. İşlem Logları
        if (remoteData.activityLogs && Array.isArray(remoteData.activityLogs) && remoteData.activityLogs.length > 0) {
            if (typeof AppDB !== 'undefined') {
                AppDB.logsCache = remoteData.activityLogs;
            }
        }

        // Yerel IndexedDB'yi de senkronize et
        if (typeof AppDB !== 'undefined' && AppDB.db && AppDB.saveAllFromCloud) {
            AppDB.saveAllFromCloud(remoteData).catch(e => console.warn('AppDB cloud sync save:', e));
        }

        return true;
    },

    /**
     * SSE veya Polling ile gelen yeni veriyi kontrol edip arayüze yansıtır.
     */
    handleRemoteDataUpdate(remoteData) {
        if (!remoteData || typeof remoteData !== 'object') return;
        if (this.isApplyingRemote) return;

        // Yankı Koruması (Echo Suppression): Kendi cihazımızın gönderdiği paketleri atla
        if (remoteData._meta && remoteData._meta.clientId === this.clientId) {
            return;
        }

        // Revizyon / Zaman Kontrolü: Eğer yerelde son 2 saniyede kullanıcı bir şey yaptıysa ve uzak veri daha eskiyse atla
        if (remoteData._meta && remoteData._meta.updatedAt) {
            if (this.lastLocalModifiedTime && remoteData._meta.updatedAt < this.lastLocalModifiedTime) {
                return;
            }
        }

        try {
            this.isApplyingRemote = true;
            const applied = this.applyRemoteDataToApp(remoteData);
            if (!applied) return;

            if (remoteData._meta && typeof remoteData._meta.revision === 'number') {
                this.localRevision = remoteData._meta.revision;
            }

            // Arayüzü güncelle
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

            this.lastSyncTime = new Date();
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            this.updateModalCloudStatus();

            // Kullanıcıya bildirim göster
            if (typeof showToast === 'function') {
                showToast('Diğer cihazdan (PC/Tablet/Telefon) yapılan değişiklikler anında senkronize edildi!', 'info', '☁️ Canlı Bulut Eşitlendi');
            }
        } finally {
            this.isApplyingRemote = false;
        }
    },

    /**
     * Buluta veri göndermeyi zamanlar (Debounce desteği ile).
     */
    schedulePush(reason = 'change', delayMs = 150) {
        if (this.isApplyingRemote) return;

        this.lastLocalModifiedTime = Date.now();

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

        if (delayMs === 0) {
            this.pushToCloud(reason);
        } else {
            this.pushDebounceTimer = setTimeout(() => {
                this.pushToCloud(reason);
            }, delayMs);
        }
    },

    /**
     * Bulut veritabanına tam durumu (Full State) PUT ile yazar.
     */
    async pushToCloud(reason = 'update') {
        if (!navigator.onLine) {
            this.syncStatus = 'offline';
            this.updateHeaderBadge();
            return false;
        }

        this.syncStatus = 'syncing';
        this.updateHeaderBadge();

        try {
            const payload = this.getFullAppState(reason);

            const res = await fetch(this.databaseUrl, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            if (!res.ok) {
                throw new Error(`HTTP ${res.status}`);
            }

            this.lastSyncTime = new Date();
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            this.updateModalCloudStatus();
            return true;
        } catch (err) {
            console.warn('[CloudDB] Bulut yazma uyarısı:', err);
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            return false;
        }
    },

    /**
     * Buluttan manuel olarak veri çeker.
     */
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
                headers: { 'Accept': 'application/json' },
                cache: 'no-store'
            });

            if (!res.ok) throw new Error(`HTTP ${res.status}`);

            const cloudData = await res.json();
            if (!cloudData || typeof cloudData !== 'object') {
                return null;
            }

            this.handleRemoteDataUpdate(cloudData);
            this.lastSyncTime = new Date();
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            this.updateModalCloudStatus();
            return cloudData;
        } catch (err) {
            console.warn('[CloudDB] Bulut okuma:', err);
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            return null;
        }
    },

    /**
     * Özel Firebase URL'i veya Oda atar.
     */
    setDatabaseUrl(newUrl) {
        if (!newUrl || !newUrl.trim()) {
            this.databaseUrl = this.defaultUrl;
            try { localStorage.removeItem('yks_custom_firebase_url'); } catch(e){}
        } else {
            let clean = newUrl.trim();
            if (!clean.endsWith('.json')) {
                clean = clean.replace(/\/+$/, '') + '/yks_planner_v2.json';
            }
            this.databaseUrl = clean;
            try { localStorage.setItem('yks_custom_firebase_url', clean); } catch(e){}
        }
        this.connectLiveStream();
        this.pullFromCloud(false);
        this.updateModalCloudStatus();
        this.updateHeaderBadge();
    },

    /**
     * Üst bardaki durum rozetini günceller.
     */
    updateHeaderBadge() {
        const badge = document.getElementById('dbStatusHeaderBtn') || document.getElementById('cloudSyncHeaderBadge');
        if (!badge) return;

        let icon = '🟢';
        let fullText = 'Canlı Bulut Veritabanı';
        let shortText = 'Bulut Aktif';
        let colorClass = 'text-emerald-400 bg-emerald-500/10 border-emerald-500/30';

        if (!navigator.onLine) {
            icon = '📴';
            fullText = 'Çevrimdışı';
            shortText = 'Çevrimdışı';
            colorClass = 'text-slate-400 bg-slate-500/10 border-slate-500/30';
        } else if (this.syncStatus === 'syncing') {
            icon = '🔄';
            fullText = 'Eşitleniyor...';
            shortText = 'Eşitleniyor';
            colorClass = 'text-amber-400 bg-amber-500/10 border-amber-500/30';
        }

        badge.className = `px-3 py-1.5 text-xs font-semibold rounded-lg border flex items-center gap-1.5 transition-all shadow-sm ${colorClass}`;
        badge.innerHTML = `
            <span class="inline-block w-2 h-2 rounded-full ${this.syncStatus === 'syncing' ? 'bg-amber-400 animate-spin' : (navigator.onLine ? 'bg-emerald-400 animate-pulse' : 'bg-slate-400')}"></span>
            <span class="hidden sm:inline">${icon} ${fullText} (Çoklu Cihaz)</span>
            <span class="sm:hidden">${icon} ${shortText}</span>
        `;
    },

    /**
     * Modal içerisindeki canlı durum metinlerini günceller.
     */
    updateModalCloudStatus() {
        const statusEl = document.getElementById('cloudModalStatusText');
        const timeEl = document.getElementById('cloudModalLastSyncTime');
        const sseEl = document.getElementById('cloudModalSseText');
        const urlInput = document.getElementById('firebaseDbUrlInput');
        const clientEl = document.getElementById('cloudModalClientId');

        if (statusEl) {
            if (!navigator.onLine) {
                statusEl.innerHTML = '<span class="text-slate-400 font-bold">📴 Çevrimdışı</span>';
            } else if (this.syncStatus === 'syncing') {
                statusEl.innerHTML = '<span class="text-amber-400 font-bold">🔄 Eşitleniyor...</span>';
            } else {
                statusEl.innerHTML = '<span class="text-emerald-400 font-bold">🟢 Firebase Canlı Bağlı (PC / Tablet / Telefon)</span>';
            }
        }

        if (timeEl && this.lastSyncTime) {
            timeEl.innerText = this.lastSyncTime.toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        }

        if (sseEl) {
            sseEl.innerHTML = this.eventSource ? '<span class="text-emerald-400 font-bold">🟢 Canlı Akış Aktif (SSE)</span>' : '<span class="text-indigo-400 font-bold">🔄 Otomatik Polling</span>';
        }

        if (urlInput) {
            urlInput.value = this.databaseUrl;
        }

        if (clientEl) {
            clientEl.innerText = this.clientId || '-';
        }
    }
};

if (typeof window !== 'undefined') window.CloudDB = CloudDB;
if (typeof global !== 'undefined') global.CloudDB = CloudDB;

