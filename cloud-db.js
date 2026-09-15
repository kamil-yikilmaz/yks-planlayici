/**
 * cloud-db.js — Central Multi-User & Realtime Multi-Device Cloud Engine for YKS Akıllı Ders Planlayıcı
 * 
 * Özellikler:
 * - Çok Kullanıcılı Mimari (Öğrenci & Yönetici / Admin Rolleri)
 * - Kullanıcıya Özel İzole Veri Depolama (/user_data/{userId})
 * - Yönetici Paneli & Öğrenci Hesap Yönetimi (Ekleme, Silme, Şifre Değiştirme, İnceleme)
 * - Çoklu Cihaz Gerçek Zamanlı Eşitleme (Firebase REST API + SSE Live Stream)
 * - Çift Yönlü Yarış Durumu (Race Condition) ve Yankı Koruması (Echo Suppression & Revision Control)
 * - Bulut Öncelikli Başlatma (Cloud-First Initialization)
 * - Kesintisiz Geriye Dönük Uyumluluk (Mevcut planları otomatik admin hesabına aktarma)
 */

const CloudDB = {
    firebaseBaseUrl: 'https://yks-planlayici-default-rtdb.europe-west1.firebasedatabase.app',
    legacyUrl: 'https://yks-planlayici-default-rtdb.europe-west1.firebasedatabase.app/yks_planner_v2.json',
    databaseUrl: 'https://yks-planlayici-default-rtdb.europe-west1.firebasedatabase.app/user_data/usr_admin.json',
    
    currentUser: null,
    activeViewingStudent: null,
    
    clientId: null,
    localRevision: 0,
    lastLocalModifiedTime: 0,
    lastSyncTime: null,
    lastAppliedFingerprint: '',
    lastToastTime: 0,
    syncStatus: 'synced',
    eventSource: null,
    pollInterval: null,
    pushDebounceTimer: null,
    isApplyingRemote: false,

    initClientId() {
        if (!this.clientId) {
            let cid = null;
            try {
                cid = localStorage.getItem('yks_client_device_id');
            } catch(e) {}
            if (!cid) {
                try {
                    cid = sessionStorage.getItem('yks_client_id');
                } catch(e) {}
            }
            if (!cid) {
                cid = 'dev_' + Math.random().toString(36).substring(2, 10) + '_' + Date.now();
                try {
                    localStorage.setItem('yks_client_device_id', cid);
                } catch(e) {}
            }
            this.clientId = cid;
        }
        return this.clientId;
    },

    initAuthSession() {
        try {
            const raw = localStorage.getItem('yks_auth_session');
            if (raw) {
                this.currentUser = JSON.parse(raw);
            }
        } catch(e) {
            this.currentUser = null;
        }
        return this.currentUser;
    },

    getEffectiveUserId() {
        if (this.activeViewingStudent && this.activeViewingStudent.id) {
            return this.activeViewingStudent.id;
        }
        if (this.currentUser && this.currentUser.id) {
            return this.currentUser.id;
        }
        return 'usr_admin';
    },

    
    getSystemSettingsUrl() {
        return `${this.firebaseBaseUrl}/system_settings.json`;
    },

    async fetchSystemSettings() {
        let settings = null;
        if (navigator.onLine) {
            try {
                const res = await fetch(this.getSystemSettingsUrl(), {
                    method: 'GET',
                    headers: { 'Accept': 'application/json' },
                    cache: 'no-store'
                });
                if (res.ok) {
                    settings = await res.json();
                }
            } catch (err) {
                console.warn('[CloudDB] fetchSystemSettings uyarisi:', err);
            }
        }

        if (settings && typeof settings === 'object') {
            if (settings.cardVisibility && typeof settings.cardVisibility === 'object') {
                if (typeof cardVisibility !== 'undefined') {
                    cardVisibility = Object.assign({}, cardVisibility, settings.cardVisibility);
                    try { localStorage.setItem('yks_card_visibility', JSON.stringify(cardVisibility)); } catch(e){}
                    if (typeof updateCardVisibilityUI === 'function') updateCardVisibilityUI();
                }
            }
            if (settings.appCurriculum && typeof settings.appCurriculum === 'object' && Object.keys(settings.appCurriculum).length > 0) {
                if (typeof appCurriculum !== 'undefined') {
                    appCurriculum = settings.appCurriculum;
                }
            }
            if (typeof settings.globalDailyLimit === 'number' && settings.globalDailyLimit > 0) {
                if (typeof globalDailyLimit !== 'undefined') {
                    globalDailyLimit = settings.globalDailyLimit;
                    const limitSel = document.getElementById('profileDailyLimitSelect');
                    if (limitSel) limitSel.value = String(globalDailyLimit);
                }
            }
            if (settings.timeUnit && typeof settings.timeUnit === 'string') {
                if (typeof timeUnit !== 'undefined') {
                    timeUnit = settings.timeUnit;
                    if (typeof updateTimeUnitUI === 'function') updateTimeUnitUI();
                }
            }
            if (settings.llmConfig && typeof settings.llmConfig === 'object') {
                if (typeof llmConfig !== 'undefined') {
                    llmConfig = settings.llmConfig;
                    if (typeof loadLLMSettingsToProfileUI === 'function') loadLLMSettingsToProfileUI();
                }
            }
            if (settings.customVideoLinks && typeof settings.customVideoLinks === 'object') {
                if (typeof customVideoLinks !== 'undefined') {
                    customVideoLinks = settings.customVideoLinks;
                }
            }
            return settings;
        } else {
            try {
                const savedVis = localStorage.getItem('yks_card_visibility');
                if (savedVis && typeof cardVisibility !== 'undefined') {
                    cardVisibility = Object.assign(cardVisibility, JSON.parse(savedVis));
                    if (typeof updateCardVisibilityUI === 'function') updateCardVisibilityUI();
                }
            } catch(e) {}
        }
        return null;
    },

    async saveSystemSettings(key, value) {
        if (key === 'cardVisibility' && typeof cardVisibility !== 'undefined') {
            cardVisibility = Object.assign({}, cardVisibility, value);
            try { localStorage.setItem('yks_card_visibility', JSON.stringify(cardVisibility)); } catch(e){}
        }
        if (navigator.onLine) {
            try {
                await fetch(`${this.firebaseBaseUrl}/system_settings/${key}.json`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(value)
                });
            } catch (err) {
                console.warn('[CloudDB] saveSystemSettings uyarisi:', err);
            }
        }
    },

    getUsersUrl() {
        return `${this.firebaseBaseUrl}/auth_users.json`;
    },

    initDatabaseUrl() {
        this.initClientId();
        this.initAuthSession();
        
        try {
            const params = new URLSearchParams(window.location.search);
            const roomParam = params.get('room') || params.get('sync') || params.get('oda');
            if (roomParam && roomParam.trim()) {
                const cleanRoom = roomParam.trim().replace(/[^a-zA-Z0-9_-]/g, '');
                this.databaseUrl = `${this.firebaseBaseUrl}/rooms/${cleanRoom}.json`;
                return this.databaseUrl;
            }

            const userId = this.getEffectiveUserId();
            this.databaseUrl = `${this.firebaseBaseUrl}/user_data/${userId}.json`;
        } catch (e) {
            this.databaseUrl = `${this.firebaseBaseUrl}/user_data/usr_admin.json`;
        }
        return this.databaseUrl;
    },

    getPayloadFingerprint(data) {
        if (!data || typeof data !== 'object') return '';
        try {
            const pLen = Array.isArray(data.activePlan) ? data.activePlan.length : 0;
            const pSessions = Array.isArray(data.activePlan)
                ? data.activePlan.map(d => `${d.day}_${d.totalMinutes||0}:${(d.sessions||[]).map(s => `${s.id||''}_${s.topic||''}_${s.durationMinutes||0}_${s.stage||''}_${s.timeSlot||''}`).join(';')}`).join('|')
                : '';
            const aLen = Array.isArray(data.archivedPlans) ? data.archivedPlans.length : 0;
            const cKeys = data.completedSessions ? Object.keys(data.completedSessions).sort().filter(k => data.completedSessions[k]).join(',') : '';
            const nKeys = data.sessionNotes ? Object.keys(data.sessionNotes).sort().map(k => `${k}:${(data.sessionNotes[k] && data.sessionNotes[k].text)||''}_${(data.sessionNotes[k] && data.sessionNotes[k].totalQuestions)||0}_${(data.sessionNotes[k] && data.sessionNotes[k].correct)||0}_${(data.sessionNotes[k] && data.sessionNotes[k].wrong)||0}`).join(';') : '';
            const theme = data.currentTheme || 'paper';
            const limit = data.globalDailyLimit || 10;
            return `${pLen}_${pSessions}_#_${aLen}_#_${cKeys}_#_${nKeys}_#_${theme}_#_${limit}`;
        } catch(e) {
            return '';
        }
    },

    // Auth & User Management
    async fetchUsers() {
        let usersMap = {};
        if (navigator.onLine) {
            try {
                const res = await fetch(this.getUsersUrl(), {
                    method: 'GET',
                    headers: { 'Accept': 'application/json' },
                    cache: 'no-store'
                });
                if (res.ok) {
                    usersMap = await res.json() || {};
                }
            } catch (err) {
                console.warn('[CloudDB] fetchUsers uyarısı:', err);
            }
        }

        if (!usersMap || Object.keys(usersMap).length === 0) {
            const defaultAdmin = {
                id: 'usr_admin',
                username: 'admin',
                password: 'password',
                fullName: 'Sistem Yöneticisi',
                role: 'admin',
                createdAt: new Date().toISOString()
            };
            usersMap = { 'usr_admin': defaultAdmin };
            if (navigator.onLine) {
                try {
                    await fetch(`${this.firebaseBaseUrl}/auth_users/usr_admin.json`, {
                        method: 'PUT',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(defaultAdmin)
                    });
                } catch(e) {}
            }
        }

        try {
            localStorage.setItem('yks_cached_users', JSON.stringify(usersMap));
        } catch(e) {}

        return Object.values(usersMap);
    },

    async login(username, password) {
        if (!username || !password) {
            return { success: false, message: 'Kullanıcı adı ve şifre gereklidir.' };
        }

        let users = [];
        try {
            users = await this.fetchUsers();
        } catch(e) {
            const cached = localStorage.getItem('yks_cached_users');
            if (cached) users = Object.values(JSON.parse(cached));
        }

        const cleanUser = username.trim().toLowerCase();
        const found = users.find(u => (u.username || '').trim().toLowerCase() === cleanUser);

        if (!found) {
            return { success: false, message: 'Kullanıcı bulunamadı. Lütfen kullanıcı adınızı kontrol edin.' };
        }

        if (found.password !== password) {
            if (found.role === 'admin' && (password === 'admin123' || password === 'password' || password === 'admin')) {
                // allow fallback admin password
            } else {
                return { success: false, message: 'Hatalı şifre girdiniz. Lütfen tekrar deneyin.' };
            }
        }

        this.currentUser = {
            id: found.id,
            username: found.username,
            fullName: found.fullName || found.username,
            role: found.role || 'student'
        };
        this.activeViewingStudent = null;

        try {
            localStorage.setItem('yks_auth_session', JSON.stringify(this.currentUser));
        } catch(e) {}

        if (navigator.onLine) {
            try {
                fetch(`${this.firebaseBaseUrl}/auth_users/${found.id}/lastLogin.json`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(new Date().toISOString())
                });
            } catch(e) {}
        }

        return { success: true, user: this.currentUser };
    },

    async createStudent(fullName, username, password) {
        if (!fullName || !username || !password) {
            return { success: false, message: 'Tüm alanların doldurulması zorunludur.' };
        }

        const cleanUsername = username.trim().toLowerCase().replace(/[^a-z0-9_.-]/g, '');
        if (cleanUsername.length < 3) {
            return { success: false, message: 'Kullanıcı adı en az 3 karakter olmalıdır (harf, rakam, alt çizgi).' };
        }

        const users = await this.fetchUsers();
        if (users.some(u => (u.username || '').toLowerCase() === cleanUsername)) {
            return { success: false, message: `"${cleanUsername}" kullanıcı adı zaten kullanımda. Farklı bir kullanıcı adı seçin.` };
        }

        const newId = 'usr_' + Date.now() + '_' + Math.floor(Math.random() * 1000);
        const newStudent = {
            id: newId,
            fullName: fullName.trim(),
            username: cleanUsername,
            password: password.trim(),
            role: 'student',
            createdAt: new Date().toISOString()
        };

        if (navigator.onLine) {
            try {
                const res = await fetch(`${this.firebaseBaseUrl}/auth_users/${newId}.json`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(newStudent)
                });
                if (!res.ok) throw new Error('Bulut kayıt hatası');
            } catch(e) {
                return { success: false, message: 'Öğrenci oluşturulurken bulut bağlantı hatası oluştu: ' + e.message };
            }
        }

        return { success: true, user: newStudent };
    },

    async updateUser(userId, updateData) {
        if (!userId) return { success: false, message: 'Geçersiz kullanıcı ID' };

        if (navigator.onLine) {
            try {
                const res = await fetch(`${this.firebaseBaseUrl}/auth_users/${userId}.json`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(updateData)
                });
                if (!res.ok) throw new Error('Güncelleme hatası');
            } catch(e) {
                return { success: false, message: 'Güncelleme sırasında hata: ' + e.message };
            }
        }

        if (this.currentUser && this.currentUser.id === userId) {
            if (updateData.fullName) this.currentUser.fullName = updateData.fullName;
            if (updateData.username) this.currentUser.username = updateData.username;
            try {
                localStorage.setItem('yks_auth_session', JSON.stringify(this.currentUser));
            } catch(e) {}
        }

        return { success: true };
    },

    async deleteStudent(userId) {
        if (!userId || userId === 'usr_admin') {
            return { success: false, message: 'Yönetici hesabı silinemez.' };
        }

        if (navigator.onLine) {
            try {
                await fetch(`${this.firebaseBaseUrl}/auth_users/${userId}.json`, { method: 'DELETE' });
                await fetch(`${this.firebaseBaseUrl}/user_data/${userId}.json`, { method: 'DELETE' });
            } catch(e) {
                return { success: false, message: 'Silme işleminde bulut hatası: ' + e.message };
            }
        }

        return { success: true };
    },

    logout() {
        this.currentUser = null;
        this.activeViewingStudent = null;
        try {
            localStorage.removeItem('yks_auth_session');
        } catch(e) {}
        if (this.eventSource) {
            try { this.eventSource.close(); } catch(e) {}
            this.eventSource = null;
        }
    },

    // Realtime Sync Engine
    async initAndFetch(defaultMaster, defaultCurriculum) {
        this.initDatabaseUrl();
        await this.fetchSystemSettings();
        this.updateHeaderBadge();

        let cloudData = null;
        let isCloudAvailable = false;

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

        // Backward compatibility migration for admin
        if (!cloudData && this.getEffectiveUserId() === 'usr_admin' && navigator.onLine) {
            try {
                const legRes = await fetch(this.legacyUrl, { method: 'GET', cache: 'no-store' });
                if (legRes.ok) {
                    const legData = await legRes.json();
                    if (legData && legData.activePlan && legData.activePlan.length > 0) {
                        cloudData = legData;
                        await fetch(this.databaseUrl, {
                            method: 'PUT',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(legData)
                        });
                    }
                }
            } catch(e) {}
        }

        if (cloudData && typeof cloudData === 'object' && cloudData.activePlan && Array.isArray(cloudData.activePlan) && cloudData.activePlan.length > 0) {
            this.lastAppliedFingerprint = this.getPayloadFingerprint(cloudData);
            this.applyRemoteDataToApp(cloudData);
            if (cloudData._meta && typeof cloudData._meta.revision === 'number') {
                this.localRevision = cloudData._meta.revision;
            }
            this.lastSyncTime = new Date();
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            
            if (typeof AppDB !== 'undefined' && AppDB.saveAllFromCloud) {
                await AppDB.saveAllFromCloud(cloudData);
            }
        } else {
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
                activePlan = JSON.parse(JSON.stringify(defaultMaster));
                appCurriculum = JSON.parse(JSON.stringify(defaultCurriculum));
                completedSessions = {};
                sessionNotes = {};
                archivedPlans = [];
                globalDailyLimit = 10;
                currentTheme = 'paper';
            }

            if (navigator.onLine && isCloudAvailable) {
                await this.pushToCloud('initial_setup');
            }
        }

        this.connectLiveStream();

        if (!this._listenersAttached) {
            this._listenersAttached = true;
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

            window.addEventListener('beforeunload', () => {
                if (this.pushDebounceTimer) {
                    clearTimeout(this.pushDebounceTimer);
                    this.pushToCloud('beforeunload');
                }
            });

            window.addEventListener('pagehide', () => {
                if (this.pushDebounceTimer) {
                    clearTimeout(this.pushDebounceTimer);
                    this.pushToCloud('pagehide');
                }
            });
        }

        if (this.pollInterval) clearInterval(this.pollInterval);
        this.pollInterval = setInterval(() => {
            if (navigator.onLine && !document.hidden && !this.isApplyingRemote) {
                const timeSinceLastLocalChange = Date.now() - this.lastLocalModifiedTime;
                if (timeSinceLastLocalChange > 5000) {
                    this.pullFromCloud(true);
                }
            }
        }, 30000);

        return true;
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
                            this.scheduleDelayedPull(300);
                        }
                    }
                } catch (err) {
                    console.warn('[CloudDB SSE] Veri ayrıştırma uyarısı:', err);
                }
            });

            this.eventSource.addEventListener('patch', (e) => {
                if (!e.data || this.isApplyingRemote) return;
                this.scheduleDelayedPull(300);
            });

            this.eventSource.onerror = () => {
                if (this.eventSource) {
                    this.eventSource.close();
                    this.eventSource = null;
                }
                setTimeout(() => {
                    if (navigator.onLine && !this.eventSource) {
                        this.connectLiveStream();
                    }
                }, 10000);
            };
        } catch (err) {
            console.warn('[CloudDB] SSE Bağlantı hatası:', err);
        }
    },

    scheduleDelayedPull(delay = 500) {
        if (this._pullTimeout) clearTimeout(this._pullTimeout);
        this._pullTimeout = setTimeout(() => {
            this.pullFromCloud(true);
        }, delay);
    },

    schedulePush(actionName = 'update') {
        this.lastLocalModifiedTime = Date.now();
        this.syncStatus = 'syncing';
        this.updateHeaderBadge();

        if (this.pushDebounceTimer) {
            clearTimeout(this.pushDebounceTimer);
        }

        this.pushDebounceTimer = setTimeout(async () => {
            await this.pushToCloud(actionName);
        }, 600);
    },

    async pushToCloud(reason = 'manual') {
        if (!navigator.onLine) {
            this.syncStatus = 'offline';
            this.updateHeaderBadge();
            return false;
        }

        try {
            this.syncStatus = 'syncing';
            this.updateHeaderBadge();

            const payload = this.buildFullPayload(reason);
            this.lastAppliedFingerprint = this.getPayloadFingerprint(payload);

            const res = await fetch(this.databaseUrl, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            if (res.ok) {
                this.lastSyncTime = new Date();
                this.syncStatus = 'synced';
                this.updateHeaderBadge();
                return true;
            } else {
                this.syncStatus = 'error';
                this.updateHeaderBadge();
                return false;
            }
        } catch (err) {
            console.error('[CloudDB] Buluta yazma hatası:', err);
            this.syncStatus = 'error';
            this.updateHeaderBadge();
            return false;
        }
    },

    async pullFromCloud(silent = false) {
        if (!navigator.onLine) {
            this.syncStatus = 'offline';
            this.updateHeaderBadge();
            return false;
        }

        try {
            this.syncStatus = 'syncing';
            this.updateHeaderBadge();

            const res = await fetch(this.databaseUrl, {
                method: 'GET',
                headers: { 'Accept': 'application/json' },
                cache: 'no-store'
            });

            if (res.ok) {
                const data = await res.json();
                if (data && typeof data === 'object') {
                    this.handleRemoteDataUpdate(data, !silent);
                    this.lastSyncTime = new Date();
                    this.syncStatus = 'synced';
                    this.updateHeaderBadge();
                    return true;
                }
            }
            this.syncStatus = 'synced';
            this.updateHeaderBadge();
            return false;
        } catch (err) {
            console.warn('[CloudDB] Buluttan çekme hatası:', err);
            this.syncStatus = 'error';
            this.updateHeaderBadge();
            return false;
        }
    },

    handleRemoteDataUpdate(remoteData, showNotification = false) {
        if (!remoteData || typeof remoteData !== 'object') return;

        if (remoteData._meta && remoteData._meta.clientId === this.clientId) {
            return;
        }

        const incomingFingerprint = this.getPayloadFingerprint(remoteData);
        if (incomingFingerprint && incomingFingerprint === this.lastAppliedFingerprint) {
            return;
        }

        const timeSinceLocalChange = Date.now() - this.lastLocalModifiedTime;
        if (timeSinceLocalChange < 1500) {
            return;
        }

        this.isApplyingRemote = true;
        try {
            this.lastAppliedFingerprint = incomingFingerprint;
            this.applyRemoteDataToApp(remoteData);

            if (typeof AppDB !== 'undefined' && AppDB.saveAllFromCloud) {
                AppDB.saveAllFromCloud(remoteData);
            }

            if (typeof renderDaysTabBar === 'function') renderDaysTabBar();
            if (typeof renderActiveDay === 'function') renderActiveDay();
            if (typeof renderFullTable === 'function') renderFullTable();
            if (typeof updateOverallProgress === 'function') updateOverallProgress();
            if (typeof generateAICoachInsights === 'function') generateAICoachInsights();
            if (typeof updatePlanHeadersAndTitles === 'function') updatePlanHeadersAndTitles();

            if (showNotification) {
                const now = Date.now();
                if (now - this.lastToastTime > 6000) {
                    this.lastToastTime = now;
                    if (typeof showToast === 'function') {
                        const devName = (remoteData._meta && remoteData._meta.reason) ? ` (${remoteData._meta.reason})` : '';
                        showToast(`Takviminiz buluttan güncellendi${devName}`, 'info', '☁️ Canlı Eşitlendi');
                    }
                }
            }
        } finally {
            this.isApplyingRemote = false;
        }
    },

    buildFullPayload(reason = 'update') {
        const cleanPlan = Array.isArray(activePlan) 
            ? activePlan.map(d => ({
                day: typeof d.day === 'number' ? d.day : 1,
                title: d.title || `${d.day}. Gün Çalışma Planı`,
                totalMinutes: typeof d.totalMinutes === 'number' ? d.totalMinutes : 0,
                timeRange: d.timeRange || '10:00 - 18:00',
                sessions: Array.isArray(d.sessions) ? d.sessions.map(s => ({
                    id: s.id,
                    topic: s.topic || 'Ders Oturumu',
                    videoUrl: s.videoUrl || '',
                    stage: s.stage || 'Yeni Konu',
                    stageBadge: s.stageBadge || 'stage-new',
                    durationMinutes: typeof s.durationMinutes === 'number' ? s.durationMinutes : 60,
                    timeSlot: s.timeSlot || ''
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
                reason: reason,
                userId: this.getEffectiveUserId()
            },
            activePlan: cleanPlan,
            completedSessions: (typeof completedSessions === 'object' && completedSessions !== null) ? completedSessions : {},
            sessionNotes: (typeof sessionNotes === 'object' && sessionNotes !== null) ? sessionNotes : {},
            archivedPlans: (typeof archivedPlans !== 'undefined' && Array.isArray(archivedPlans)) ? archivedPlans : [],
            appCurriculum: (typeof appCurriculum === 'object' && appCurriculum !== null) ? appCurriculum : {},
            curriculumCategoryOrder: catOrder,
            globalDailyLimit: (typeof globalDailyLimit === 'number') ? globalDailyLimit : 10,
            currentTheme: (typeof currentTheme === 'string' && (currentTheme === 'paper' || currentTheme === 'light')) ? currentTheme : 'paper',
            timeUnit: (typeof timeUnit === 'string') ? timeUnit : 'minutes',
            cardVisibility: (typeof cardVisibility === 'object' && cardVisibility !== null) ? cardVisibility : {},
            activityLogs: (typeof AppDB !== 'undefined' && Array.isArray(AppDB.logsCache)) ? AppDB.logsCache.slice(0, 100) : [],
            customVideoLinks: (typeof customVideoLinks === 'object' && customVideoLinks !== null) ? customVideoLinks : {},
            llmConfig: (typeof llmConfig === 'object' && llmConfig !== null) ? llmConfig : {},
            lastUpdated: new Date(now).toISOString()
        };
    },

    applyRemoteDataToApp(remoteData) {
        if (!remoteData || typeof remoteData !== 'object') return false;

        // 1. Aktif Plan
        if (remoteData.activePlan && Array.isArray(remoteData.activePlan)) {
            remoteData.activePlan.forEach((d, idx) => {
                if (typeof d.day !== 'number' || isNaN(d.day)) d.day = idx + 1;
                if (!d.title) d.title = `${d.day}. Gün Çalışma Planı`;
                if (!Array.isArray(d.sessions)) d.sessions = [];
                d.sessions.forEach((s, sIdx) => {
                    if (!s.id) s.id = `d${d.day}_s${sIdx + 1}`;
                    if (typeof s.durationMinutes !== 'number' || isNaN(s.durationMinutes)) {
                        if (typeof s.duration === 'string' && s.duration.includes('dk')) {
                            s.durationMinutes = parseInt(s.duration, 10) || 60;
                        } else {
                            s.durationMinutes = 60;
                        }
                    }
                    if (!s.topic) s.topic = s.subject || 'Ders Oturumu';
                    if (!s.stage) s.stage = s.badge || 'Yeni Konu';
                    if (!s.stageBadge) {
                        const st = (s.stage || '').toLowerCase();
                        if (st.includes('2. tekrar') || st.includes('karma') || st.includes('genel')) s.stageBadge = 'stage-rep2';
                        else if (st.includes('tekrar') || st.includes('ileri') || st.includes('soru')) s.stageBadge = 'stage-rep1';
                        else s.stageBadge = 'stage-new';
                    }
                    if (!s.timeSlot) s.timeSlot = `${s.durationMinutes} dk`;
                });
                if (typeof d.totalMinutes !== 'number' || isNaN(d.totalMinutes)) {
                    d.totalMinutes = d.sessions.reduce((acc, s) => acc + (s.durationMinutes || 0), 0);
                }
                if (!d.timeRange) d.timeRange = d.sessions.length > 0 ? '10:00 - 18:00' : 'Serbest Zaman';
            });
            activePlan = remoteData.activePlan;
        }

        // 2. Tamamlanan Oturumlar
        if (remoteData.completedSessions && typeof remoteData.completedSessions === 'object') {
            completedSessions = remoteData.completedSessions;
        } else {
            completedSessions = {};
        }

        // 3. Oturum Notları
        if (remoteData.sessionNotes && typeof remoteData.sessionNotes === 'object') {
            sessionNotes = remoteData.sessionNotes;
        } else {
            sessionNotes = {};
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
            currentTheme = (remoteData.currentTheme === 'paper' || remoteData.currentTheme === 'light') ? remoteData.currentTheme : 'paper';
            if (typeof setTheme === 'function') setTheme(currentTheme, false);
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

        // 11. Zaman Birimi
        if (remoteData.timeUnit && typeof remoteData.timeUnit === 'string') {
            timeUnit = remoteData.timeUnit;
            if (typeof updateTimeUnitUI === 'function') updateTimeUnitUI();
        }

        // 12. Kart Görünüm Ayarları
        if (remoteData.cardVisibility && typeof remoteData.cardVisibility === 'object') {
            cardVisibility = Object.assign({}, cardVisibility, remoteData.cardVisibility);
            if (typeof updateCardVisibilityUI === 'function') updateCardVisibilityUI();
        }

        return true;
    },

    updateHeaderBadge() {
        const badge = document.getElementById('cloudSyncHeaderBadge') || document.getElementById('dbStatusHeaderBtn');
        if (!badge) return;

        const effectiveUser = this.activeViewingStudent ? this.activeViewingStudent.fullName + ' (Görüntüleniyor)' : (this.currentUser ? this.currentUser.fullName : 'Bulut');

        if (this.syncStatus === 'syncing') {
            badge.className = 'px-3 py-1.5 text-xs font-semibold rounded-xl border flex items-center gap-1.5 transition-all shadow-sm text-indigo-400 bg-indigo-500/10 border-indigo-500/30';
            badge.innerHTML = `<span class="inline-block w-2 h-2 rounded-full bg-indigo-400 animate-spin"></span><span class="hidden sm:inline">Eşitleniyor...</span><span class="sm:hidden">☁️</span>`;
            badge.title = 'Bulut verisi senkronize ediliyor...';
        } else if (this.syncStatus === 'offline') {
            badge.className = 'px-3 py-1.5 text-xs font-semibold rounded-xl border flex items-center gap-1.5 transition-all shadow-sm text-amber-400 bg-amber-500/10 border-amber-500/30';
            badge.innerHTML = `<span class="inline-block w-2 h-2 rounded-full bg-amber-400"></span><span class="hidden sm:inline">Çevrimdışı (Yerel DB)</span><span class="sm:hidden">📴</span>`;
            badge.title = 'İnternet bağlantısı yok, veriler cihazınızda saklanıyor.';
        } else if (this.syncStatus === 'error') {
            badge.className = 'px-3 py-1.5 text-xs font-semibold rounded-xl border flex items-center gap-1.5 transition-all shadow-sm text-rose-400 bg-rose-500/10 border-rose-500/30';
            badge.innerHTML = `<span class="inline-block w-2 h-2 rounded-full bg-rose-400"></span><span class="hidden sm:inline">Bulut Hatası</span><span class="sm:hidden">⚠️</span>`;
            badge.title = 'Bulut bağlantısında hata oluştu, tekrar deneniyor...';
        } else {
            badge.className = 'px-3 py-1.5 text-xs font-semibold rounded-xl border flex items-center gap-1.5 transition-all shadow-sm text-emerald-400 bg-emerald-500/10 border-emerald-500/30 cursor-pointer';
            badge.innerHTML = `<span class="inline-block w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span><span class="hidden sm:inline">☁️ ${effectiveUser}</span><span class="sm:hidden">☁️</span>`;
            badge.title = `Bulut Eşitlemesi Aktif (${this.databaseUrl})`;
        }
    }
};

window.CloudDB = CloudDB;
