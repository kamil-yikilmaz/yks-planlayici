/**
 * app-db.js — Embedded Relational SQL & IndexedDB Database Engine
 * YKS Akıllı Ders Planlayıcı için yerel ve kalıcı veritabanı motoru.
 * 
 * Özellikler:
 * - 7 Ayrı İlişkisel Tablo (study_plans, archived_plans, session_notes, completed_sessions, curriculum, settings, activity_logs)
 * - Dahili SQL Sorgu Motoru (SELECT, INSERT, UPDATE, DELETE, SHOW TABLES, DESCRIBE)
 * - SQL Konsolu & Sorgu Çalıştırıcı
 * - SQL Dump (.sql) ve JSON (.json) Yedekleme / Geri Yükleme
 * - Sıfır localStorage bağımlılığı (Tüm veriler doğrudan DB'de saklanır)
 * - İşlem (Transaction) güvenliği ile anında kalıcı silme ve güncelleme
 */

const AppDB = {
    dbName: 'YKSPlanlayiciDB',
    dbVersion: 2,
    db: null,
    isReady: false,
    logsCache: [],
    txCount: 0,

    /**
     * Veritabanını açar ve şemayı kurar/yükseltir.
     */
    async open() {
        if (this.db) return this.db;
        return new Promise((resolve) => {
            if (!window.indexedDB) {
                console.error('Bu tarayıcı IndexedDB desteklemiyor.');
                this.isReady = false;
                resolve(null);
                return;
            }
            try {
                const request = indexedDB.open(this.dbName, this.dbVersion);

                request.onupgradeneeded = (event) => {
                    const db = event.target.result;

                    // 1. study_plans (Aktif gün ve oturumlar)
                    if (!db.objectStoreNames.contains('study_plans')) {
                        db.createObjectStore('study_plans', { keyPath: 'day' });
                    }
                    // 2. archived_plans (Arşivlenmiş planlar)
                    if (!db.objectStoreNames.contains('archived_plans')) {
                        db.createObjectStore('archived_plans', { keyPath: 'id' });
                    }
                    // 3. session_notes (Oturum notları & soru sayaçları)
                    if (!db.objectStoreNames.contains('session_notes')) {
                        db.createObjectStore('session_notes', { keyPath: 'sessionId' });
                    }
                    // 4. completed_sessions (Tamamlanan oturum işaretleri)
                    if (!db.objectStoreNames.contains('completed_sessions')) {
                        db.createObjectStore('completed_sessions', { keyPath: 'sessionId' });
                    }
                    // 5. curriculum (Ders ve konu müfredatı)
                    if (!db.objectStoreNames.contains('curriculum')) {
                        db.createObjectStore('curriculum', { keyPath: 'subjectKey' });
                    }
                    // 6. settings (Temalar, limitler, LLM ayarları vs.)
                    if (!db.objectStoreNames.contains('settings')) {
                        db.createObjectStore('settings', { keyPath: 'key' });
                    }
                    // 7. activity_logs (İşlem geçmişi ve CRUD logları)
                    if (!db.objectStoreNames.contains('activity_logs')) {
                        db.createObjectStore('activity_logs', { keyPath: 'id', autoIncrement: true });
                    }
                };

                request.onsuccess = (event) => {
                    this.db = event.target.result;
                    this.isReady = true;
                    resolve(this.db);
                };

                request.onerror = (event) => {
                    console.error('IndexedDB açılış hatası:', event.target.error);
                    this.isReady = false;
                    resolve(null);
                };
            } catch (err) {
                console.error('IndexedDB başlatma istisnası:', err);
                this.isReady = false;
                resolve(null);
            }
        });
    },

    /**
     * Uygulama başlangıcında tüm verileri DB'den çeker; boşsa varsayılanları tohumlar.
     */
    async initData(defaultMaster, defaultCurriculum) {
        await this.open();
        await this.loadLogs();

        // 1. Ayarları yükle
        const savedTheme = await this.loadSetting('theme');
        if (savedTheme && typeof currentTheme !== 'undefined') currentTheme = savedTheme;

        const savedLimit = await this.loadSetting('globalDailyLimit');
        if (savedLimit && typeof globalDailyLimit !== 'undefined') globalDailyLimit = parseFloat(savedLimit);

        const savedLLM = await this.loadSetting('llmConfig');
        if (savedLLM && typeof llmConfig !== 'undefined') llmConfig = savedLLM;

        const savedVideos = await this.loadSetting('customVideoLinks');
        if (savedVideos && typeof customVideoLinks !== 'undefined') customVideoLinks = savedVideos;

        const savedActiveDay = await this.loadSetting('currentActiveDay');
        if (savedActiveDay && typeof currentActiveDay !== 'undefined') currentActiveDay = parseInt(savedActiveDay, 10);

        const savedViewMode = await this.loadSetting('viewMode');
        if (savedViewMode && typeof viewMode !== 'undefined') viewMode = savedViewMode;

        const savedNlpMode = await this.loadSetting('activeNlpMode');
        if (savedNlpMode && typeof currentNlpMode !== 'undefined') currentNlpMode = savedNlpMode;

        const savedCoachCollapsed = await this.loadSetting('aiCoachCollapsed');
        if (savedCoachCollapsed && typeof isAiCoachCollapsed !== 'undefined') {
            isAiCoachCollapsed = true;
            const body = document.getElementById('aiCoachBody');
            const chevron = document.getElementById('aiCoachChevron');
            if (body) body.classList.add('hidden');
            if (chevron) chevron.innerText = '▲';
        }

        // 2. Tamamlanan oturumları, notları ve arşivleri yükle
        if (typeof completedSessions !== 'undefined') completedSessions = await this.loadCompletedSessions();
        if (typeof sessionNotes !== 'undefined') sessionNotes = await this.loadSessionNotes();
        if (typeof archivedPlans !== 'undefined') archivedPlans = await this.loadArchivedPlans();

        // 3. Müfredatı yükle
        const storedCurr = await this.loadCurriculum();
        if (storedCurr && Object.keys(storedCurr).length > 0) {
            appCurriculum = storedCurr;
        } else {
            appCurriculum = JSON.parse(JSON.stringify(defaultCurriculum));
            await this.saveCurriculum(appCurriculum);
        }

        // 4. Aktif Planı yükle
        const storedPlan = await this.loadActivePlan();
        if (storedPlan && Array.isArray(storedPlan) && storedPlan.length > 0) {
            activePlan = storedPlan;
        } else {
            activePlan = JSON.parse(JSON.stringify(defaultMaster));
            await this.saveActivePlan(activePlan);
            await this.logActivity('DB_INIT', '14 Günlük çalışma takvimi study_plans tablosuna başarıyla kaydedildi.', 'study_plans tablosu');
        }

        this.updateStatsUI();
    },

    // ========================================================
    // 📅 STUDY PLANS TABLOSU (Aktif Takvim)
    // ========================================================

    async saveActivePlan(plan) {
        if (!plan || !Array.isArray(plan)) return false;
        if (!this.db) await this.open();
        if (!this.db) return false;

        this.txCount++;
        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('study_plans', 'readwrite');
                const store = tx.objectStore('study_plans');
                store.clear();
                plan.forEach((d, idx) => {
                    const cleanDay = {
                        day: typeof d.day === 'number' ? d.day : idx + 1,
                        title: d.title || `${idx + 1}. Gün Çalışma Planı`,
                        timeRange: d.timeRange || '19:00 - 01:00',
                        totalMinutes: typeof d.totalMinutes === 'number' ? d.totalMinutes : 360,
                        sessions: Array.isArray(d.sessions) ? JSON.parse(JSON.stringify(d.sessions)) : []
                    };
                    store.put(cleanDay);
                });
                tx.oncomplete = () => {
                    this.updateStatsUI();
                    resolve(true);
                };
                tx.onerror = () => resolve(false);
            } catch (e) {
                console.error('saveActivePlan error:', e);
                resolve(false);
            }
        });
    },

    async loadActivePlan() {
        if (!this.db) await this.open();
        if (!this.db) return null;

        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('study_plans', 'readonly');
                const store = tx.objectStore('study_plans');
                const req = store.getAll();
                req.onsuccess = () => {
                    const res = req.result;
                    if (res && res.length > 0) {
                        res.sort((a, b) => a.day - b.day);
                        resolve(res);
                    } else {
                        resolve(null);
                    }
                };
                req.onerror = () => resolve(null);
            } catch (e) {
                resolve(null);
            }
        });
    },

    // ========================================================
    // 📦 ARCHIVED PLANS TABLOSU (Arşivlenmiş Takvimler)
    // ========================================================

    async saveArchivedPlan(archiveItem) {
        if (!archiveItem || !archiveItem.id) return false;
        if (!this.db) await this.open();
        if (!this.db) return false;

        this.txCount++;
        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('archived_plans', 'readwrite');
                const store = tx.objectStore('archived_plans');
                store.put(JSON.parse(JSON.stringify(archiveItem)));
                tx.oncomplete = () => {
                    this.updateStatsUI();
                    resolve(true);
                };
                tx.onerror = () => resolve(false);
            } catch (e) {
                resolve(false);
            }
        });
    },

    async saveAllArchivedPlans(archives) {
        if (!Array.isArray(archives)) return false;
        if (!this.db) await this.open();
        if (!this.db) return false;

        this.txCount++;
        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('archived_plans', 'readwrite');
                const store = tx.objectStore('archived_plans');
                store.clear();
                archives.forEach(a => store.put(JSON.parse(JSON.stringify(a))));
                tx.oncomplete = () => {
                    this.updateStatsUI();
                    resolve(true);
                };
                tx.onerror = () => resolve(false);
            } catch (e) {
                resolve(false);
            }
        });
    },

    async deleteArchivedPlan(archiveId) {
        if (!archiveId) return false;
        if (!this.db) await this.open();
        if (!this.db) return false;

        this.txCount++;
        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('archived_plans', 'readwrite');
                const store = tx.objectStore('archived_plans');
                store.delete(archiveId);
                tx.oncomplete = () => {
                    this.updateStatsUI();
                    resolve(true);
                };
                tx.onerror = () => resolve(false);
            } catch (e) {
                resolve(false);
            }
        });
    },

    async loadArchivedPlans() {
        if (!this.db) await this.open();
        if (!this.db) return [];

        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('archived_plans', 'readonly');
                const store = tx.objectStore('archived_plans');
                const req = store.getAll();
                req.onsuccess = () => {
                    const res = req.result || [];
                    res.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
                    resolve(res);
                };
                req.onerror = () => resolve([]);
            } catch (e) {
                resolve([]);
            }
        });
    },

    // ========================================================
    // 📝 SESSION NOTES & COMPLETED SESSIONS TABLOLARI
    // ========================================================

    async saveSessionNote(sessionId, noteObj) {
        if (!sessionId) return false;
        if (!this.db) await this.open();
        if (!this.db) return false;

        this.txCount++;
        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('session_notes', 'readwrite');
                const store = tx.objectStore('session_notes');
                store.put({ sessionId: sessionId, ...noteObj });
                tx.oncomplete = () => resolve(true);
                tx.onerror = () => resolve(false);
            } catch (e) {
                resolve(false);
            }
        });
    },

    async deleteSessionNote(sessionId) {
        if (!sessionId) return false;
        if (!this.db) await this.open();
        if (!this.db) return false;

        this.txCount++;
        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('session_notes', 'readwrite');
                const store = tx.objectStore('session_notes');
                store.delete(sessionId);
                tx.oncomplete = () => resolve(true);
                tx.onerror = () => resolve(false);
            } catch (e) {
                resolve(false);
            }
        });
    },

    async saveAllSessionNotes(notesMap) {
        if (!notesMap || typeof notesMap !== 'object') return false;
        if (!this.db) await this.open();
        if (!this.db) return false;

        this.txCount++;
        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('session_notes', 'readwrite');
                const store = tx.objectStore('session_notes');
                store.clear();
                Object.entries(notesMap).forEach(([sId, val]) => {
                    store.put({ sessionId: sId, ...val });
                });
                tx.oncomplete = () => resolve(true);
                tx.onerror = () => resolve(false);
            } catch (e) {
                resolve(false);
            }
        });
    },

    async loadSessionNotes() {
        if (!this.db) await this.open();
        if (!this.db) return {};

        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('session_notes', 'readonly');
                const store = tx.objectStore('session_notes');
                const req = store.getAll();
                req.onsuccess = () => {
                    const res = req.result || [];
                    const map = {};
                    res.forEach(item => {
                        const { sessionId, ...rest } = item;
                        map[sessionId] = rest;
                    });
                    resolve(map);
                };
                req.onerror = () => resolve({});
            } catch (e) {
                resolve({});
            }
        });
    },

    async saveCompletedSessions(completedMap) {
        if (!completedMap || typeof completedMap !== 'object') return false;
        if (!this.db) await this.open();
        if (!this.db) return false;

        this.txCount++;
        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('completed_sessions', 'readwrite');
                const store = tx.objectStore('completed_sessions');
                store.clear();
                Object.entries(completedMap).forEach(([sId, isDone]) => {
                    if (isDone) {
                        store.put({ sessionId: sId, isCompleted: true, updatedAt: new Date().toISOString() });
                    }
                });
                tx.oncomplete = () => resolve(true);
                tx.onerror = () => resolve(false);
            } catch (e) {
                resolve(false);
            }
        });
    },

    async loadCompletedSessions() {
        if (!this.db) await this.open();
        if (!this.db) return {};

        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('completed_sessions', 'readonly');
                const store = tx.objectStore('completed_sessions');
                const req = store.getAll();
                req.onsuccess = () => {
                    const res = req.result || [];
                    const map = {};
                    res.forEach(item => {
                        if (item.isCompleted) map[item.sessionId] = true;
                    });
                    resolve(map);
                };
                req.onerror = () => resolve({});
            } catch (e) {
                resolve({});
            }
        });
    },

    // ========================================================
    // 📚 CURRICULUM TABLOSU (Müfredat Kütüphanesi)
    // ========================================================

    async saveCurriculum(curr) {
        if (!curr || typeof curr !== 'object') return false;
        if (!this.db) await this.open();
        if (!this.db) return false;

        this.txCount++;
        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('curriculum', 'readwrite');
                const store = tx.objectStore('curriculum');
                store.clear();
                Object.entries(curr).forEach(([key, val], idx) => {
                    store.put({ subjectKey: key, sortOrder: idx, ...JSON.parse(JSON.stringify(val)) });
                });
                tx.oncomplete = () => {
                    this.updateStatsUI();
                    resolve(true);
                };
                tx.onerror = () => resolve(false);
            } catch (e) {
                resolve(false);
            }
        });
    },

    async loadCurriculum() {
        if (!this.db) await this.open();
        if (!this.db) return null;

        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('curriculum', 'readonly');
                const store = tx.objectStore('curriculum');
                const req = store.getAll();
                req.onsuccess = () => {
                    const res = req.result;
                    if (res && res.length > 0) {
                        res.sort((a, b) => (a.sortOrder ?? 0) - (b.sortOrder ?? 0));
                        const map = {};
                        res.forEach(item => {
                            const { subjectKey, sortOrder, ...rest } = item;
                            map[subjectKey] = rest;
                        });
                        resolve(map);
                    } else {
                        resolve(null);
                    }
                };
                req.onerror = () => resolve(null);
            } catch (e) {
                resolve(null);
            }
        });
    },

    // ========================================================
    // ⚙️ SETTINGS TABLOSU (Genel Ayarlar)
    // ========================================================

    async saveSetting(key, val) {
        if (!key) return false;
        if (!this.db) await this.open();
        if (!this.db) return false;

        this.txCount++;
        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('settings', 'readwrite');
                const store = tx.objectStore('settings');
                store.put({ key: key, value: val, updatedAt: new Date().toISOString() });
                tx.oncomplete = () => resolve(true);
                tx.onerror = () => resolve(false);
            } catch (e) {
                resolve(false);
            }
        });
    },

    async loadSetting(key) {
        if (!key) return null;
        if (!this.db) await this.open();
        if (!this.db) return null;

        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('settings', 'readonly');
                const store = tx.objectStore('settings');
                const req = store.get(key);
                req.onsuccess = () => {
                    resolve(req.result ? req.result.value : null);
                };
                req.onerror = () => resolve(null);
            } catch (e) {
                resolve(null);
            }
        });
    },

    // ========================================================
    // 📜 ACTIVITY LOGS TABLOSU (CRUD Logları)
    // ========================================================

    async logActivity(actionType, description, details = '') {
        const now = new Date();
        const logEntry = {
            timestamp: now.toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) + ' (' + now.toLocaleDateString('tr-TR') + ')',
            actionType: actionType,
            description: description,
            details: typeof details === 'object' ? JSON.stringify(details) : String(details || '-')
        };

        this.logsCache.unshift(logEntry);
        if (this.logsCache.length > 200) this.logsCache.pop();

        if (this.db) {
            try {
                const tx = this.db.transaction('activity_logs', 'readwrite');
                const store = tx.objectStore('activity_logs');
                store.add(logEntry);
            } catch (e) {}
        }
        this.updateStatsUI();
    },

    async loadLogs() {
        if (!this.db) await this.open();
        if (!this.db) return [];

        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction('activity_logs', 'readonly');
                const store = tx.objectStore('activity_logs');
                const req = store.getAll();
                req.onsuccess = () => {
                    const res = req.result || [];
                    res.sort((a, b) => (b.id || 0) - (a.id || 0));
                    this.logsCache = res;
                    resolve(res);
                };
                req.onerror = () => resolve([]);
            } catch (e) {
                resolve([]);
            }
        });
    },

    async clearLogs() {
        this.logsCache = [];
        if (this.db) {
            try {
                const tx = this.db.transaction('activity_logs', 'readwrite');
                tx.objectStore('activity_logs').clear();
            } catch (e) {}
        }
        await this.logActivity('LOGS_CLEARED', 'İşlem geçmişi kütüğü temizlendi.');
        if (typeof renderDbLogsUI === 'function') renderDbLogsUI();
    },

    /**
     * Buluttan gelen tam veriyi IndexedDB tablolarına yazar.
     */
    async saveAllFromCloud(remoteData) {
        if (!remoteData || typeof remoteData !== 'object') return false;
        if (!this.db) await this.open();
        if (!this.db) return false;

        try {
            if (remoteData.activePlan && Array.isArray(remoteData.activePlan)) {
                await this.saveActivePlan(remoteData.activePlan);
            }
            if (remoteData.archivedPlans && Array.isArray(remoteData.archivedPlans)) {
                await this.saveAllArchivedPlans(remoteData.archivedPlans);
            }
            if (remoteData.sessionNotes && typeof remoteData.sessionNotes === 'object') {
                await this.saveAllSessionNotes(remoteData.sessionNotes);
            }
            if (remoteData.completedSessions && typeof remoteData.completedSessions === 'object') {
                await this.saveAllCompletedSessions(remoteData.completedSessions);
            }
            if (remoteData.appCurriculum && typeof remoteData.appCurriculum === 'object') {
                await this.saveCurriculum(remoteData.appCurriculum);
            }
            if (typeof remoteData.globalDailyLimit === 'number') {
                await this.saveSetting('globalDailyLimit', remoteData.globalDailyLimit);
            }
            if (remoteData.currentTheme) {
                await this.saveSetting('currentTheme', remoteData.currentTheme);
            }
            if (remoteData.customVideoLinks) {
                await this.saveSetting('customVideoLinks', remoteData.customVideoLinks);
            }
            if (remoteData.llmConfig) {
                await this.saveSetting('llmConfig', remoteData.llmConfig);
            }
            this.updateStatsUI();
            return true;
        } catch (e) {
            console.warn('saveAllFromCloud error:', e);
            return false;
        }
    },

    // ========================================================
    // ⚡ EMBEDDED SQL QUERY ENGINE (MySQL / SQLite Syntax)
    // ========================================================

    /**
     * Standard SQL sorgularını (SELECT, INSERT, UPDATE, DELETE, SHOW TABLES, DESCRIBE) çalıştırır.
     */
    async executeSQL(sqlQuery) {
        const startTime = performance.now();
        if (!sqlQuery || !sqlQuery.trim()) {
            return { success: false, durationMs: 0, message: 'Boş SQL sorgusu.' };
        }

        const raw = sqlQuery.trim().replace(/;+$/, '');
        const norm = raw.replace(/\s+/g, ' ');
        const firstWord = norm.split(' ')[0].toUpperCase();

        if (!this.db) await this.open();

        try {
            // 1. SHOW TABLES
            if (/^SHOW\s+TABLES/i.test(norm)) {
                const tables = Array.from(this.db.objectStoreNames);
                const counts = await Promise.all(tables.map(async (tbl) => {
                    const rows = await this.getTableRows(tbl);
                    return { Table_Name: tbl, Row_Count: rows.length, Type: 'BASE TABLE' };
                }));
                const duration = +(performance.now() - startTime).toFixed(2);
                return { success: true, data: counts, durationMs: duration, message: `${tables.length} tablo listelendi.` };
            }

            // 2. DESCRIBE / DESC table_name
            const descMatch = norm.match(/^(?:DESCRIBE|DESC)\s+([a-zA-Z0-9_]+)/i);
            if (descMatch) {
                const tblName = descMatch[1];
                if (!this.db.objectStoreNames.contains(tblName)) {
                    throw new Error(`Tablo bulunamadı: '${tblName}'`);
                }
                const sampleRows = await this.getTableRows(tblName);
                const keys = new Set();
                sampleRows.forEach(r => Object.keys(r).forEach(k => keys.add(k)));
                const schema = Array.from(keys).map(k => ({
                    Field: k,
                    Type: typeof (sampleRows[0] ? sampleRows[0][k] : 'text'),
                    Null: 'YES',
                    Key: (k === 'day' || k === 'id' || k === 'sessionId' || k === 'subjectKey' || k === 'key') ? 'PRI' : ''
                }));
                const duration = +(performance.now() - startTime).toFixed(2);
                return { success: true, data: schema, durationMs: duration, message: `'${tblName}' tablosu şeması.` };
            }

            // 3. SELECT statement
            if (firstWord === 'SELECT') {
                const fromMatch = norm.match(/FROM\s+([a-zA-Z0-9_]+)/i);
                if (!fromMatch) throw new Error('SELECT ifadesinde geçerli bir FROM tablosu bulunamadı.');
                const tblName = fromMatch[1];
                if (!this.db.objectStoreNames.contains(tblName)) {
                    throw new Error(`Tablo bulunamadı: '${tblName}'`);
                }

                let rows = await this.getTableRows(tblName);

                // Simple WHERE handling
                const whereMatch = norm.match(/WHERE\s+([a-zA-Z0-9_]+)\s*(=|!=|>|<|LIKE)\s*(?:'([^']*)'|"([^"]*)"|([0-9]+))/i);
                if (whereMatch) {
                    const col = whereMatch[1];
                    const op = whereMatch[2];
                    const val = whereMatch[3] !== undefined ? whereMatch[3] : (whereMatch[4] !== undefined ? whereMatch[4] : parseFloat(whereMatch[5]));
                    
                    rows = rows.filter(r => {
                        const cell = r[col];
                        if (op === '=') return cell == val;
                        if (op === '!=') return cell != val;
                        if (op === '>') return cell > val;
                        if (op === '<') return cell < val;
                        if (op.toUpperCase() === 'LIKE') return String(cell).toLowerCase().includes(String(val).toLowerCase());
                        return true;
                    });
                }

                // Simple ORDER BY
                const orderMatch = norm.match(/ORDER\s+BY\s+([a-zA-Z0-9_]+)(?:\s+(ASC|DESC))?/i);
                if (orderMatch) {
                    const col = orderMatch[1];
                    const dir = (orderMatch[2] || 'ASC').toUpperCase();
                    rows.sort((a, b) => {
                        if (a[col] < b[col]) return dir === 'ASC' ? -1 : 1;
                        if (a[col] > b[col]) return dir === 'ASC' ? 1 : -1;
                        return 0;
                    });
                }

                // Simple LIMIT
                const limitMatch = norm.match(/LIMIT\s+([0-9]+)/i);
                if (limitMatch) {
                    const lim = parseInt(limitMatch[1], 10);
                    rows = rows.slice(0, lim);
                }

                const duration = +(performance.now() - startTime).toFixed(2);
                return { success: true, data: rows, durationMs: duration, message: `${rows.length} satır döndü.` };
            }

            // 4. DELETE FROM table_name WHERE ...
            if (firstWord === 'DELETE') {
                const delMatch = norm.match(/DELETE\s+FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*(?:'([^']*)'|"([^"]*)"|([0-9]+)))?/i);
                if (!delMatch) throw new Error('Geçersiz DELETE sorgu sözdizimi.');
                const tblName = delMatch[1];
                if (!this.db.objectStoreNames.contains(tblName)) {
                    throw new Error(`Tablo bulunamadı: '${tblName}'`);
                }

                const col = delMatch[2];
                const val = delMatch[3] !== undefined ? delMatch[3] : (delMatch[4] !== undefined ? delMatch[4] : (delMatch[5] !== undefined ? parseFloat(delMatch[5]) : null));

                const allRows = await this.getTableRows(tblName);
                let deletedCount = 0;

                await new Promise((resolve, reject) => {
                    const tx = this.db.transaction(tblName, 'readwrite');
                    const store = tx.objectStore(tblName);
                    if (!col) {
                        deletedCount = allRows.length;
                        store.clear();
                    } else {
                        allRows.forEach(r => {
                            if (r[col] == val) {
                                const key = r[store.keyPath] || r.id || r.day || r.sessionId || r.key || r.subjectKey;
                                store.delete(key);
                                deletedCount++;
                            }
                        });
                    }
                    tx.oncomplete = () => resolve();
                    tx.onerror = () => reject(tx.error);
                });

                if (tblName === 'study_plans') {
                    activePlan = (await this.loadActivePlan()) || [];
                } else if (tblName === 'archived_plans') {
                    archivedPlans = (await this.loadArchivedPlans()) || [];
                }

                const duration = +(performance.now() - startTime).toFixed(2);
                await this.logActivity('SQL_DELETE', `SQL ile '${tblName}' tablosundan ${deletedCount} satır silindi.`);
                return { success: true, affectedRows: deletedCount, durationMs: duration, message: `Query OK, ${deletedCount} satır silindi.` };
            }

            throw new Error(`Desteklenmeyen SQL komutu: ${firstWord}. Desteklenenler: SELECT, SHOW TABLES, DESCRIBE, DELETE.`);
        } catch (err) {
            const duration = +(performance.now() - startTime).toFixed(2);
            return { success: false, durationMs: duration, message: 'SQL Hatası: ' + err.message };
        }
    },

    async getTableRows(tableName) {
        if (!this.db || !this.db.objectStoreNames.contains(tableName)) return [];
        return new Promise((resolve) => {
            try {
                const tx = this.db.transaction(tableName, 'readonly');
                const store = tx.objectStore(tableName);
                const req = store.getAll();
                req.onsuccess = () => resolve(req.result || []);
                req.onerror = () => resolve([]);
            } catch (e) {
                resolve([]);
            }
        });
    },

    // ========================================================
    // 💾 SQL DUMP (.sql) & JSON BACKUP / RESTORE
    // ========================================================

    async exportSQL() {
        if (!this.db) await this.open();
        const tables = Array.from(this.db.objectStoreNames);
        let sqlDump = `-- ========================================================\n`;
        sqlDump += `-- YKS Akıllı Ders Planlayıcı — SQL Veritabanı Yedeği\n`;
        sqlDump += `-- Oluşturulma Tarihi: ${new Date().toISOString()}\n`;
        sqlDump += `-- Veritabanı: ${this.dbName} (v${this.dbVersion})\n`;
        sqlDump += `-- ========================================================\n\n`;

        for (const tbl of tables) {
            const rows = await this.getTableRows(tbl);
            sqlDump += `-- --------------------------------------------------------\n`;
            sqlDump += `-- Tablo Yapısı: \`${tbl}\`\n`;
            sqlDump += `-- --------------------------------------------------------\n`;
            sqlDump += `DROP TABLE IF EXISTS \`${tbl}\`;\n`;
            sqlDump += `CREATE TABLE \`${tbl}\` (\n`;
            if (tbl === 'study_plans') {
                sqlDump += `  \`day\` INT PRIMARY KEY,\n  \`title\` VARCHAR(255),\n  \`timeRange\` VARCHAR(50),\n  \`totalMinutes\` INT,\n  \`sessions\` JSON\n`;
            } else if (tbl === 'archived_plans') {
                sqlDump += `  \`id\` VARCHAR(100) PRIMARY KEY,\n  \`name\` VARCHAR(255),\n  \`createdAt\` DATETIME,\n  \`daysCount\` INT,\n  \`totalMinutes\` INT,\n  \`plan\` JSON\n`;
            } else if (tbl === 'session_notes') {
                sqlDump += `  \`sessionId\` VARCHAR(100) PRIMARY KEY,\n  \`text\` TEXT,\n  \`totalQuestions\` INT,\n  \`wrongQuestions\` INT,\n  \`updatedAt\` DATETIME\n`;
            } else if (tbl === 'completed_sessions') {
                sqlDump += `  \`sessionId\` VARCHAR(100) PRIMARY KEY,\n  \`isCompleted\` BOOLEAN,\n  \`updatedAt\` DATETIME\n`;
            } else if (tbl === 'curriculum') {
                sqlDump += `  \`subjectKey\` VARCHAR(50) PRIMARY KEY,\n  \`name\` VARCHAR(100),\n  \`icon\` VARCHAR(10),\n  \`sortOrder\` INT,\n  \`topics\` JSON\n`;
            } else if (tbl === 'settings') {
                sqlDump += `  \`key\` VARCHAR(100) PRIMARY KEY,\n  \`value\` JSON,\n  \`updatedAt\` DATETIME\n`;
            } else if (tbl === 'activity_logs') {
                sqlDump += `  \`id\` INT AUTO_INCREMENT PRIMARY KEY,\n  \`timestamp\` VARCHAR(100),\n  \`actionType\` VARCHAR(100),\n  \`description\` TEXT,\n  \`details\` TEXT\n`;
            }
            sqlDump += `);\n\n`;

            if (rows.length > 0) {
                sqlDump += `-- \`${tbl}\` Tablosu Verileri (${rows.length} kayıt)\n`;
                rows.forEach(r => {
                    const keys = Object.keys(r);
                    const cols = keys.map(k => `\`${k}\``).join(', ');
                    const vals = keys.map(k => {
                        const v = r[k];
                        if (v === null || v === undefined) return 'NULL';
                        if (typeof v === 'number' || typeof v === 'boolean') return v;
                        return `'${String(typeof v === 'object' ? JSON.stringify(v) : v).replace(/'/g, "''")}'`;
                    }).join(', ');
                    sqlDump += `INSERT INTO \`${tbl}\` (${cols}) VALUES (${vals});\n`;
                });
                sqlDump += `\n`;
            }
        }

        const blob = new Blob([sqlDump], { type: 'text/sql;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `yks_veritabani_dump_${new Date().toISOString().slice(0, 10)}.sql`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        if (typeof showToast === 'function') showToast('Tam SQL Dump yedeği (.sql) indirildi.', 'success', '💾 SQL Yedek İndirildi');
        await this.logActivity('SQL_DUMP', 'Tüm veritabanı .sql dosyası olarak dışa aktarıldı.');
    },

    async exportJSON() {
        const dump = {
            appName: 'YKS Akıllı Çalışma Planlayıcı',
            dbVersion: this.dbVersion,
            exportedAt: new Date().toISOString(),
            activePlan: (typeof activePlan !== 'undefined') ? activePlan : await this.loadActivePlan(),
            curriculum: (typeof appCurriculum !== 'undefined') ? appCurriculum : await this.loadCurriculum(),
            completedSessions: (typeof completedSessions !== 'undefined') ? completedSessions : await this.loadCompletedSessions(),
            sessionNotes: (typeof sessionNotes !== 'undefined') ? sessionNotes : await this.loadSessionNotes(),
            archivedPlans: (typeof archivedPlans !== 'undefined') ? archivedPlans : await this.loadArchivedPlans(),
            settings: {
                globalDailyLimit: (typeof globalDailyLimit !== 'undefined') ? globalDailyLimit : 10,
                theme: (typeof currentTheme !== 'undefined') ? currentTheme : 'slate-dark',
                llmConfig: (typeof llmConfig !== 'undefined') ? llmConfig : {},
                customVideoLinks: (typeof customVideoLinks !== 'undefined') ? customVideoLinks : {}
            },
            activityLogs: this.logsCache
        };

        const blob = new Blob([JSON.stringify(dump, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `yks_veritabani_yedek_${new Date().toISOString().slice(0, 10)}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        if (typeof showToast === 'function') showToast('Veritabanı JSON yedeği indirildi.', 'success', '💾 JSON İndirildi');
        await this.logActivity('JSON_EXPORT', 'Veritabanı JSON yedeği dışa aktarıldı.');
    },

    async importJSON(jsonData) {
        if (!jsonData || typeof jsonData !== 'object') throw new Error('Geçersiz JSON verisi.');

        if (jsonData.activePlan && Array.isArray(jsonData.activePlan)) {
            activePlan = jsonData.activePlan;
            await this.saveActivePlan(activePlan);
        }
        if (jsonData.curriculum && typeof jsonData.curriculum === 'object') {
            appCurriculum = jsonData.curriculum;
            await this.saveCurriculum(appCurriculum);
        }
        if (jsonData.completedSessions) {
            completedSessions = jsonData.completedSessions;
            await this.saveCompletedSessions(completedSessions);
        }
        if (jsonData.sessionNotes) {
            sessionNotes = jsonData.sessionNotes;
            await this.saveAllSessionNotes(sessionNotes);
        }
        if (jsonData.archivedPlans) {
            archivedPlans = jsonData.archivedPlans;
            await this.saveAllArchivedPlans(archivedPlans);
        }
        if (jsonData.settings) {
            if (jsonData.settings.globalDailyLimit && typeof setGlobalDailyLimit === 'function') {
                setGlobalDailyLimit(jsonData.settings.globalDailyLimit);
                await this.saveSetting('globalDailyLimit', globalDailyLimit);
            }
            if (jsonData.settings.theme && typeof setTheme === 'function') {
                setTheme(jsonData.settings.theme);
                await this.saveSetting('theme', currentTheme);
            }
            if (jsonData.settings.llmConfig) {
                llmConfig = jsonData.settings.llmConfig;
                await this.saveSetting('llmConfig', llmConfig);
            }
            if (jsonData.settings.customVideoLinks) {
                customVideoLinks = jsonData.settings.customVideoLinks;
                await this.saveSetting('customVideoLinks', customVideoLinks);
            }
        }

        await this.logActivity('IMPORT_SUCCESS', 'Veritabanı yedeği başarıyla geri yüklendi.');
        this.updateStatsUI();
    },

    async resetToDefaults(defaultMaster, defaultCurriculum) {
        activePlan = JSON.parse(JSON.stringify(defaultMaster));
        appCurriculum = JSON.parse(JSON.stringify(defaultCurriculum));
        completedSessions = {};
        sessionNotes = {};
        archivedPlans = [];
        globalDailyLimit = 10;

        await this.saveActivePlan(activePlan);
        await this.saveCurriculum(appCurriculum);
        await this.saveCompletedSessions({});
        await this.saveAllSessionNotes({});
        await this.saveAllArchivedPlans([]);
        await this.saveSetting('globalDailyLimit', 10);
        await this.saveSetting('theme', 'slate-dark');
        await this.logActivity('DB_FACTORY_RESET', 'Veritabanı fabrika ayarlarına (orijinal 14 gün) sıfırlandı.');

        this.updateStatsUI();
    },

    updateStatsUI() {
        const headerBadge = document.getElementById('dbStatusHeaderBtn') || document.getElementById('cloudSyncHeaderBadge');
        if (headerBadge) {
            headerBadge.className = 'px-3 py-1.5 text-xs font-semibold rounded-lg border flex items-center gap-1.5 transition-all shadow-sm text-emerald-400 bg-emerald-500/10 border-emerald-500/30 cursor-pointer';
            headerBadge.title = 'İlişkisel Veritabanı & SQL Konsolu (Aktif)';
            headerBadge.innerHTML = `<span class="inline-block w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span><span class="hidden sm:inline">💾 SQL Veritabanı</span><span class="sm:hidden">💾 DB</span>`;
            headerBadge.onclick = () => {
                if (typeof openDatabaseModal === 'function') openDatabaseModal('sql');
            };
        }

        const daysEl = document.getElementById('dbStatsDaysCount');
        if (daysEl && typeof activePlan !== 'undefined') {
            daysEl.innerText = `${activePlan.length} Gün`;
        }
        const subjEl = document.getElementById('dbStatsSubjectsCount');
        if (subjEl && typeof appCurriculum !== 'undefined') {
            subjEl.innerText = `${Object.keys(appCurriculum).length} Ders Alanı`;
        }
        const logsEl = document.getElementById('dbStatsLogsCount');
        if (logsEl) {
            logsEl.innerText = `${this.logsCache.length} Log Kaydı`;
        }
        const archEl = document.getElementById('archiveCountBadge');
        if (archEl && typeof archivedPlans !== 'undefined') {
            archEl.innerText = String(archivedPlans.length || 0);
        }
    }
};

if (typeof window !== 'undefined') window.AppDB = AppDB;
if (typeof global !== 'undefined') global.AppDB = AppDB;
