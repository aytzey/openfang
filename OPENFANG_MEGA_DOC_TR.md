# OpenFang Prospecting Engine — Birlesmis Mega Dokuman

> Bu dokuman 5 ayri analiz dokumaninin birlesmis halidir.
> Herhangi bir LLM veya insan gelistirici, uygulamayi hic gormeden
> sistemi tamamen anlayabilmelidir.
>
> Olusturulma: 26 Mart 2026
> Kaynak: Gercek kod analizi (sales.rs 14.466 satir, sales.js 612 satir,
> index_body.html 508 satir) + canli cikti dogrulamasi

---

# ANA ICINDEKILER

**PARCA 1** — [OpenFang Platform Genel Bakisi](#parca-1--openfang-platform-genel-bakisi)
(Kernel, Agent sistemi, Bellek, A2A/OFP protokolleri, Butce, Workflow, 40 kanal)

**PARCA 2** — [Prospecting Engine Teknik Referans](#parca-2--prospecting-engine-teknik-referans)
(38 sabit, 8 struct, 8 DB tablosu, 7 asamali pipeline, 8 dizin tarayici,
14 filtreleme fonksiyonu, 5 LLM prompt, SMTP/LinkedIn kodu, tam JS/HTML analizi)

**PARCA 3** — [Canli Veride 12 Bug + Hedef Tasarim + Ekran Wireframe](#parca-3--hedef-tasarim-ve-ekranlar)
(12 bug, 0-1000 puanlama, LLM mesaj uretimi, 6 ekran wireframe)

**PARCA 4** — [Uzman Degerlendirmesi + Guncel Implementasyon Plani](#parca-4--uzman-degerlendirmesi-ve-gercek-implementasyon-plani)
(ChatGPT Pro + Gemini + Gemini Deep Research birlesmis. Revenue Graph +
Activation Engine + Learning Loop + Multi-Agent Swarm + Niyet Motoru +
Deliverability Zirhi + Deger Uretimi + Omnichannel Durum Makinesi.
**53 maddelik 5 fazli plan**)

---

===============================================================
# PARCA 1 — OPENFANG PLATFORM GENEL BAKISI
===============================================================

# OpenFang Sistem Analizi - Kapsamli Teknik Dokumantasyon

> Bu dokuman, OpenFang Agent Isletim Sistemi'nin tum alt sistemlerini, is akislarini,
> satis motorunu, musteri profilleme mekanizmasini ve potansiyel musteri bulma
> algoritmalarini detayli olarak aciklar.

---

## Icindekiler

1. [Genel Bakis](#1-genel-bakis)
2. [Mimari Yapi ve Crate Haritasi](#2-mimari-yapi-ve-crate-haritasi)
3. [Kernel - Isletim Sistemi Cekirdegi](#3-kernel---isletim-sistemi-cekirdegi)
4. [Agent Yasam Dongusu](#4-agent-yasam-dongusu)
5. [LLM Agent Loop - Cekirdek Calisma Mekanizmasi](#5-llm-agent-loop---cekirdek-calisma-mekanizmasi)
6. [LLM Driver Sistemi - 20+ Saglayici](#6-llm-driver-sistemi---20-saglayici)
7. [Arac (Tool) Calistirma Altyapisi](#7-arac-tool-calistirma-altyapisi)
8. [Bellek (Memory) Sistemi](#8-bellek-memory-sistemi)
9. [Satis Motoru ve Musteri Profilleme](#9-satis-motoru-ve-musteri-profilleme)
10. [Potansiyel Musteri Bulma Algoritmasi](#10-potansiyel-musteri-bulma-algoritmasi)
11. [Zenginlestirme ve Profil Olusturma](#11-zenginlestirme-ve-profil-olusturma)
12. [Lead Donusumu ve Onay Akisi](#12-lead-donusumu-ve-onay-akisi)
13. [Dashboard ve Ekranlar](#13-dashboard-ve-ekranlar)
14. [Codex OAuth Entegrasyonu](#14-codex-oauth-entegrasyonu)
15. [A2A ve OFP Ag Protokolleri](#15-a2a-ve-ofp-ag-protokolleri)
16. [Butce ve Maliyet Takibi](#16-butce-ve-maliyet-takibi)
17. [Workflow - Is Akisi Motoru](#17-workflow---is-akisi-motoru)
18. [Zamanlama ve Otomasyon](#18-zamanlama-ve-otomasyon)
19. [Kanal Adapterleri - 40 Platform](#19-kanal-adapterleri---40-platform)
20. [Guvenlik Mimarisi](#20-guvenlik-mimarisi)
21. [Yapilandirma Sistemi](#21-yapilandirma-sistemi)
22. [API Endpoint Katalogu](#22-api-endpoint-katalogu)
23. [Tablo: Temel Istatistikler](#23-tablo-temel-istatistikler)

---

## 1. Genel Bakis

**OpenFang**, Rust dilinde yazilmis acik kaynakli bir **Agent Isletim Sistemi**dir.
14 Rust crate'inden olusan monorepo yapisindadir. Temel amaci: otonom AI agentlarin
olusturulmasini, yonetilmesini, birbirleriyle iletisim kurmasini ve dis dunyayla
etkilesime girmesini saglayan uretim kalitesinde bir platform sunmaktir.

### Temel Ozellikler

| Metrik | Deger |
|--------|-------|
| Crate sayisi | 14 (workspace) |
| Rust dosya sayisi | 241 |
| API endpoint sayisi | 150+ (REST/WS/SSE) |
| Mesajlasma kanali | 40 platform |
| Hazir beceri (skill) | 60+ |
| Onceden tanimli agent sablonu | 30 |
| LLM saglayicisi | 20+ |
| LLM modeli | 51+ (130+ katalogda) |
| Guvenlik alt sistemi | 16 |

### Calisma Sekli

```
Kullanici (CLI / Desktop / SDK / Web Dashboard)
         |
         v
   HTTP/WS API Sunucusu (port 4200)
         |
         v
   OpenFang Kernel (Agent Isletim Sistemi Cekirdegi)
         |
    +----+----+----+----+
    |    |    |    |    |
    v    v    v    v    v
  Agent  Bellek  Arac  Kanal  Ag
  Loop   Sistemi Runner Bridge Proto.
```

---

## 2. Mimari Yapi ve Crate Haritasi

### 2.1 Bagimlilk Akisi

```
openfang-types           (sifir bagimllik, temel tipler)
    |
    +-- openfang-memory      (SQLite + semantik arama + bilgi grafi)
    +-- openfang-wire        (OFP ag protokolu, TCP + HMAC)
    +-- openfang-channels    (40 mesajlasma adaptoru)
    +-- openfang-skills      (beceri kayit + yukleme + pazar yeri)
    +-- openfang-hands       (otonom paket agentlar)
    +-- openfang-extensions  (MCP sunucu + kimlik kasasi + OAuth)
    +-- openfang-migrate     (OpenClaw/LangChain goc araci)
    |
    +-- openfang-runtime     (LLM suruculeri, arac calistirici, sandbox)
    |       |
    |       +-- openfang-kernel  (yasam dongusu, zamanlayici, is akisi, olay bus)
    |               |
    |               +-- openfang-api     (HTTP sunucusu, tum route'lar)
    |                       |
    |                       +-- openfang-cli      (komut satiri araci)
    |                       +-- openfang-desktop  (Tauri 2.0 masaustu uygulama)
    |
    +-- xtask                (derleme otomasyon gorevleri)
```

### 2.2 Her Crate'in Gorevi

| Crate | Dosya Yolu | Temel Gorev |
|-------|-----------|-------------|
| **openfang-types** | `crates/openfang-types/` | Paylasilan veri yapilari (agent, mesaj, arac, olay, yapilandirma) |
| **openfang-memory** | `crates/openfang-memory/` | Birlesik bellek API: SQLite + semantik arama + bilgi grafi + oturum |
| **openfang-runtime** | `crates/openfang-runtime/` | LLM suruculeri (20+), arac calistirma, WASM sandbox, tarayici kontrolu |
| **openfang-wire** | `crates/openfang-wire/` | OFP mesh ag protokolu: TCP + JSON-RPC + HMAC-SHA256 dogrulama |
| **openfang-kernel** | `crates/openfang-kernel/` | Cekirdek: agent kayit defteri, zamanlayici, is akisi, olay yolu, onay |
| **openfang-api** | `crates/openfang-api/` | Axum HTTP sunucusu (150+ endpoint), WebSocket, SSE, Dashboard |
| **openfang-cli** | `crates/openfang-cli/` | Komut satiri araci + TUI panosu (ratatui) |
| **openfang-channels** | `crates/openfang-channels/` | 40 mesajlasma platformu adaptoru |
| **openfang-skills** | `crates/openfang-skills/` | Beceri sistemi: Python/WASM/Node.js/Dahili/PromptOnly |
| **openfang-hands** | `crates/openfang-hands/` | Otonom alan-uzman agent paketleri |
| **openfang-extensions** | `crates/openfang-extensions/` | Entegrasyon pazari + AES-256-GCM kimlik kasasi + OAuth2 PKCE |
| **openfang-migrate** | `crates/openfang-migrate/` | OpenClaw ve diger frameworklerden goc araci |
| **openfang-desktop** | `crates/openfang-desktop/` | Tauri 2.0 masaustu uygulamasi (macOS/Windows/Linux) |
| **xtask** | `xtask/` | Derleme ve test otomasyon gorevleri |

---

## 3. Kernel - Isletim Sistemi Cekirdegi

**Dosya:** `crates/openfang-kernel/src/kernel.rs` (~5000 satir)

Kernel, tum alt sistemleri bir araya getiren merkezi orkestrasyon katmanidir.

### 3.1 Kernel Bilesenleri

```rust
pub struct OpenFangKernel {
    config: KernelConfig,
    registry: AgentRegistry,           // Agent kayit defteri
    scheduler: AgentScheduler,         // Kaynak kota takibi
    memory: Arc<MemorySubstrate>,      // Birlesik bellek
    event_bus: EventBus,               // Olay dagitimi
    capabilities: CapabilityManager,   // Yetki yonetimi
    workflow_engine: WorkflowEngine,   // Is akisi motoru
    trigger_engine: TriggerEngine,     // Olay tetikleyicileri
    cron: CronScheduler,              // Zamanlanmis gorevler
    supervisor: Supervisor,            // Agent denetimi
    audit_log: AuditLog,              // Denetim kaydi (Merkle hash zinciri)
    metering: MeteringEngine,          // Maliyet olcumu
    model_catalog: ModelCatalog,       // 130+ model katalogu
    skill_registry: SkillRegistry,     // Beceri kayit defteri
    peer_registry: Option<PeerRegistry>, // OFP esler
    peer_node: Option<Arc<PeerNode>>,    // Ag dugumu
    a2a_task_store: A2aTaskStore,      // A2A gorev deposu
    background_executor: BackgroundExecutor, // Arka plan calistirici
}
```

### 3.2 Kernel Baslatma Sirasi

1. Yapilandirma yukle (`config.toml`)
2. Bellek alt katmanini baslat (SQLite + gomme)
3. LLM suruclerini olustur (varsayilan + yedek)
4. WASM sandbox'u baslat
5. Alt sistemleri baslat:
   - EventBus, Scheduler, CapabilityManager, TriggerEngine
   - Supervisor, AuditLog, MeteringEngine, Cron
   - Model katalogu, Skill kayit defteri, Uzantilar, MCP baglantilari
6. SQLite'tan kalici agentlari geri yukle
7. Geri yuklenen agentlara yetkiler ver
8. Yonlendirme yapilandirmalarini model kataloguyla dogrula
9. `self_handle` kur (agent ozreferans icin)
10. Basarili baslatma raporla

### 3.3 KernelHandle Trait'i

`KernelHandle`, runtime ile kernel arasinda kopruleme saglar. Agentlarin
birbirlerini bulmalarini, mesajlasmalarini ve kaynaklari paylasmalarini mumkun kilar:

```rust
pub trait KernelHandle: Send + Sync {
    // Agent Yonetimi
    async fn spawn_agent(&self, manifest_toml: &str) -> Result<AgentId>;
    async fn send_to_agent(&self, target: &str, message: &str) -> Result<String>;
    async fn list_agents(&self) -> Vec<AgentInfo>;
    async fn kill_agent(&self, agent_id: AgentId) -> Result<()>;
    async fn find_agents(&self, query: &str) -> Vec<AgentInfo>;

    // Bellek Islemleri
    async fn memory_store(&self, key: &str, value: &str) -> Result<()>;
    async fn memory_recall(&self, key: &str) -> Result<Option<String>>;

    // Gorev Kuyrugu
    async fn task_post(&self, title: &str, body: &str) -> Result<String>;
    async fn task_claim(&self) -> Result<Option<TaskItem>>;
    async fn task_complete(&self, task_id: &str, result: &str) -> Result<()>;

    // Bilgi Grafi
    async fn knowledge_add_entity(&self, ...) -> Result<()>;
    async fn knowledge_add_relation(&self, ...) -> Result<()>;
    async fn knowledge_query(&self, pattern: &str) -> Result<Vec<...>>;

    // Zamanlama
    async fn cron_create(&self, schedule: &str, prompt: &str) -> Result<String>;
    async fn cron_list(&self) -> Vec<CronJob>;

    // Onay Sistemi
    async fn requires_approval(&self, tool: &str) -> bool;
    async fn request_approval(&self, ...) -> Result<bool>;
}
```

---

## 4. Agent Yasam Dongusu

### 4.1 Agent Olusturma

```
Manifest (TOML) --> spawn_agent_with_parent()
    |
    +-- UUID ve SessionId olustur
    +-- Calisma alani dizini olustur ({workspaces_dir}/{isim}-{id_prefix}/)
    +-- Yetkiler kaydet (manifest'e gore)
    +-- Zamanlayiciya kaydet (kaynak takibi icin)
    +-- Kayit defterine ekle (durum: Running)
    +-- SQLite'a kalici kayit yaz
    +-- "Spawned" yasam dongusu olayi yayinla
```

### 4.2 Agent Durumari

| Durum | Aciklama |
|-------|----------|
| `Created` | Yeni olusturuldu, henuz calismiyor |
| `Running` | Aktif olarak olaylari isliyor |
| `Suspended` | Duraklatildi, islem yapmiyor |
| `Terminated` | Kalici olarak durduruldu |
| `Crashed` | Basarisiz oldu, kurtarma bekliyor |

### 4.3 Agent Modlari

| Mod | Araclar | Aciklama |
|-----|--------|----------|
| `Observe` | Hicbiri | Salt okunur, arac kullanilamaz |
| `Assist` | Sinirli (file_read, web_fetch, agent_list...) | Sadece okuma araclari |
| `Full` | Tumu | Tum verilen araclar kullanilabilir (varsayilan) |

### 4.4 Agent Manifest Yapisi

```rust
pub struct AgentManifest {
    name: String,
    module: String,                    // "builtin:chat", "wasm:...", "python:..."
    model: ModelConfig,                // saglayici, model, max_tokens, sicaklik
    fallback_models: Vec<FallbackModel>,
    resources: ResourceQuota,          // Kotalar
    capabilities: ManifestCapabilities, // Araclar, ag, kabuk, bellek izinleri
    skills: Vec<String>,               // Yuklu beceriler
    mcp_servers: Vec<String>,          // MCP sunucu izin listesi
    tags: Vec<String>,                 // Kategorize etme etiketleri
    routing: Option<ModelRoutingConfig>, // Karmasikliga gore model secimi
    autonomous: Option<AutonomousConfig>, // 7/24 agent koruyuculari
    exec_policy: Option<ExecPolicy>,   // Kabuk calistirma kisitlamalari
}
```

---

## 5. LLM Agent Loop - Cekirdek Calisma Mekanizmasi

**Dosya:** `crates/openfang-runtime/src/agent_loop.rs` (~1000+ satir)

Bu, sistemin kalbi. Her agent mesaji aldiginda asagidaki dongu calisir:

### 5.1 Dongu Sabitleri

```
MAX_ITERATIONS: 50        (yapilandirilabiilr)
TOOL_TIMEOUT_SECS: 120    (arac basi zaman asimi)
MAX_HISTORY_MESSAGES: 20   (otomatik kirpma)
DEFAULT_CONTEXT_WINDOW: 200,000 token
```

### 5.2 Tam Dongu Sureci

```
1. BELLEK GERI CAGIRMA
   +-- Vektor benzerlik aramasi (gomme surucu varsa)
   +-- Yoksa metin tabanli BM25 aramasi
   +-- En fazla 5 ilgili bellek getir

2. SISTEM ISTEMI OLUSTURMA
   +-- Agent manifest'inden temel istem
   +-- Geri cagrilan bellekleri ekle
   +-- Calisma alani baglami (SOUL.md, USER.md, MEMORY.md)
   +-- Beceri ozetleri, MCP arac ozetleri

3. MESAJ HAZIRLAMA
   +-- Sistem mesajlarini filtrele
   +-- Oturum gecmisini dogrula ve onar
   +-- 20'den fazlaysa kirp

4. LLM CAGRISI (yeniden deneme ile)
   +-- CompletionRequest olustur:
       * Oturumdaki mesajlar
       * Kullanilabilir araclar
       * Model yapilandirmasi
       * Sistem istemi
   +-- 3 yeniden deneme, ustel geri cekilme
   +-- Hiz siniri algilama ve isleme
   +-- Sonsuz dongu icin devre kesici

5. YANIT ISLEME

   a) StopReason::EndTurn | StopSequence:
      +-- Metin yanitini cikar
      +-- Yanit yonergelerini ayristir ([[silent]] vb.)
      +-- Oturumu belleye kaydet
      +-- Etkilesimi gomme ile hatirla
      +-- Nihai yaniti dondur

   b) StopReason::ToolUse:
      +-- Yanittaki arac cagrilarini cikar
      +-- Dongu koruyucu dogrulamasi (sonsuz dongu engeli)
      +-- HER ARAC ICIN:
        * Hook: BeforeToolCall (engelleyebilir)
        * Zaman asimi: 120 saniye
        * Yetki denetimi
        * Gerekiyorsa onay kapisi
        * tool_runner ile calistir
        * Baglam butcesine gore dinamik sonuc kirpma
        * Hook: AfterToolCall
      +-- Arac sonuclarini ToolResult olarak topla
      +-- Mesaj gecmisine User roluyle ekle
      +-- Ara oturum kaydi
      +-- DONGUNUN SONRAKI ITERASYONUNA DEVAM ET

   c) StopReason::MaxTokens:
      +-- Devam sayacini artir
      +-- 5+ devamsa: kismi yanit dondur
      +-- Degilse: donguye devam

6. BAGLAM YONETIMI
   +-- Baglam koruyucu: tasmadan once arac sonuclarini sikistir
   +-- Tasma icin kurtarma hatti
   +-- Arac sonuclari icin token tabanli butce

7. SONLANDIRMA
   +-- AgentLoopEnd hook'u ates et
   +-- Olcum katalogundan maliyeti doldur
   +-- AgentLoopResult dondur:
       * yanit metni
       * toplam token kullanimi
       * iterasyon sayisi
       * maliyet_usd
       * sessiz bayrak
```

### 5.3 Anahtar Ozellikler

- **Metin tabanli arac cagrilari kurtarma**: Groq/Llama gibi modeller icin
- **Leke takibi (Taint Tracking)**: Guvenlik icin veri akisini izler
- **Onay kapilari**: Yuksek riskli araclar icin insan onayi
- **Bellek birlestirme**: Episodik etkilesimleri uzun sureli bellege kaydeder
- **Dinamik arac sonucu kirpma**: Baglam pencresine duyarli

---

## 6. LLM Driver Sistemi - 20+ Saglayici

**Dosya:** `crates/openfang-runtime/src/llm_driver.rs` + `drivers/`

### 6.1 Driver Trait'i

```rust
pub trait LlmDriver: Send + Sync {
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse>;
    async fn stream(&self, request: CompletionRequest, tx: Sender<StreamEvent>) -> Result<CompletionResponse>;
}
```

### 6.2 Desteklenen Saglayicilar

**Ozel Surucular:**
- `anthropic` - Claude Messages API
- `gemini` - Google Generative AI
- `github-copilot` - Token degisimi + OpenAI sarmalayici
- `openai-codex` - OAuth Codex arka ucu

**OpenAI Uyumlu Surucular:**
- groq, openrouter, deepseek, together, mistral, fireworks
- openai, perplexity, cohere, ai21, cerebras, sambanova
- huggingface, xai, replicate, moonshot, qwen, minimax, zhipu, qianfan
- ollama, vllm, lmstudio (yerel)
- Ozel base_url endpoint'leri

### 6.3 Model Yonlendirme

Sorgu karmasikligina gore otomatik model secimi:
- `simple_model`: Ucuz/hizli model (kisa sorgular)
- `medium_model`: Orta seviye model
- `complex_model`: Pahali/yetenekli model (karisik sorgular)

Token tahminine dayali karmasiklik puani hesaplanir ve uygun modele yonlendirilir.

---

## 7. Arac (Tool) Calistirma Altyapisi

**Dosya:** `crates/openfang-runtime/src/tool_runner.rs` (~1500 satir)

### 7.1 Dahili Araclar

**Dosya Sistemi:**
- `file_read`, `file_write`, `file_list`, `apply_patch`

**Web:**
- `web_fetch` (SSRF korumaliu, onbellekli)
- `web_search` (Tavily, Brave, Perplexity, DuckDuckGo)

**Calistirma:**
- `shell_exec` (calistirma politikasi + leke kontrolu)
- `docker_exec` (opsiyonel sandbox)
- `process_start/poll/write/kill` (kalici REPL'ler)

**Agentlar Arasi:**
- `agent_send`, `agent_spawn`, `agent_list`, `agent_kill`, `agent_find`
- `task_post`, `task_claim`, `task_complete`, `task_list`
- `event_publish`

**Zamanlama:**
- `schedule_create/list/delete`
- `cron_create/list/cancel`

**Bilgi:**
- `knowledge_add_entity`, `knowledge_add_relation`, `knowledge_query`

**Medya:**
- `image_analyze`, `media_describe`, `media_transcribe`
- `image_generate`, `text_to_speech`, `speech_to_text`

### 7.2 Guvenlik Kontrol Akisi

```
Arac Cagrisi Geldi
    |
    v
1. YETKI DENETIMI
   +-- Arac izin listesinde mi?
   +-- Degilse: "izin reddedildi" dondur
    |
    v
2. ONAY KAPISI
   +-- requires_approval() kontrol et
   +-- Gerekiyorsa onay iste
   +-- Reddedilirse veya zaman asiminda: calistirma
    |
    v
3. LEKE TAKIBI
   +-- Kabuk: curl/wget/eval/base64 kaliplarini engelle
   +-- Ag: API anahtarlari/sirlar iceren URL'leri engelle
    |
    v
4. ZAMAN ASIMI
   +-- 120 saniye arac basi
    |
    v
5. DONGU KORUYUCU
   +-- Ayni arac + parametreler tekrarlaniyor mu?
   +-- Tekrarlanan basarisizliklar icin devre kesici
    |
    v
6. CALISTIRMA
   +-- Beceri mi? -> SkillRegistry'ye delege et
   +-- MCP mi? -> MCP baglantisina delege et
   +-- Dahili mi? -> Dahili arac calistir
```

---

## 8. Bellek (Memory) Sistemi

**Dosya:** `crates/openfang-memory/src/` (birden fazla dosya)

### 8.1 Birlesik Mimari

```rust
pub struct MemorySubstrate {
    structured: StructuredStore,      // Anahtar-deger ciftleri, agent durumu
    semantic: SemanticStore,          // Puanli metin bellekleri
    knowledge: KnowledgeStore,        // Varlik-iliski grafi
    sessions: SessionStore,           // Konusma gecmisi
    consolidation: ConsolidationEngine, // Bellek birlestirme
    usage: UsageStore,                // LLM cagri takibi
}
```

### 8.2 Semantik Bellek (Geri Cagirma Motoru)

Iki asamali hibrit arama:

**Asama 1: Deterministik Hash N-gram Arama**
- Metin normalizasyonu (NFKC, kucuk harf, noktalama birlesimi)
- 2-gram ve 3-gram hash'leri uret
- Iki tohum ile coklu kafa kararli hash'leme
- O(1) aday uretimi

**Asama 2: Baglam Duyarli Gecit**
```rust
pub struct ScoredMemoryMatch {
    score: f32,              // Nihai siralama puani
    gate: f32,               // Guven gecidi (0.0-1.0)
    lexical_confidence: f32, // Token ortusme kalitesi
    semantic_score: f32,     // Gomme benzerligii
    lexical_hits: u32,       // Eslesen terim sayisi
}
```

### 8.3 Depolama Semasi

```sql
CREATE TABLE memories (
  id TEXT PRIMARY KEY,
  agent_id TEXT,
  content TEXT,
  source TEXT,        -- conversation/document/observation/inference
  scope TEXT,         -- episodic/semantic/procedural
  confidence REAL,    -- 0.0-1.0 (zaman icinde azalir)
  metadata TEXT,      -- JSON
  embedding BLOB      -- Opsiyonel vektor
);
```

### 8.4 Bilgi Grafi

Yapilandirilmis gercek depolamasi:
- **Varliklar**: Person, Organization, Project, Concept, Event, Location, Document, Tool
- **Iliskiler**: Guven puanli varliklar arasi yazili baglantilar
- Graf desen sorgulari: kaynak -> iliski -> hedef

### 8.5 Bellek Birlestirme ve Zayiflama

```rust
// 7 gun erisilmeyen belleklerin guvenini azalt
UPDATE memories
SET confidence = MAX(0.1, confidence * (1.0 - decay_rate))
WHERE accessed_at < 7_gun_once
```

---

## 9. Satis Motoru ve Musteri Profilleme

**Dosya:** `crates/openfang-api/src/sales.rs` (~14.148 satir)

OpenFang'in en kapsamli alt sistemlerinden biri. Tam otomatik B2B satis motoru.

### 9.1 Genel Mimari

```
+-------------------------------------------------------------------+
|                     SATIS MOTORU AKISI                             |
+-------------------------------------------------------------------+
|                                                                    |
|  [1] SORGU PLANI   --> LLM veya sezgisel arama sorgulari olustur |
|        |                                                           |
|  [2] PARALEL KESIF  --> 3 paralel kaynak:                         |
|        |                  a) LLM Sirket Uretimi                   |
|        |                  b) Web Arama Kesfii                     |
|        |                  c) Turk Dizin Taramasi (TR icin)        |
|        |                                                           |
|  [3] BIRLESTIR + TEKILSIZLESTIR                                  |
|        |                                                           |
|  [4] LLM ILG. DOGRULAMA --> ICP'ye karsi adaylari puana          |
|        |                                                           |
|  [5] FILTRELE + SIRALA --> Kalite esik degeri uygula              |
|        |                                                           |
|  [6] ZENGINLESTIRME --> Site OSINT + Arama OSINT + LLM Arastirma |
|        |                                                           |
|  [7] LEAD DONUSUMU --> Iletisim bilgisi olan profiller lead olur  |
|        |                                                           |
|  [8] MESAJ TASLAGI --> E-posta ve LinkedIn mesajlari hazirla      |
|        |                                                           |
|  [9] ONAY KUYRUGU --> Insan onayina sun                           |
|        |                                                           |
| [10] TESLIM --> Onaylanan mesajlari gonder                        |
+-------------------------------------------------------------------+
```

### 9.2 Satis Profili (Kullanici Yapilandirmasi)

```rust
pub struct SalesProfile {
    product_name: String,           // Urun adi
    product_description: String,    // Urun tanimi
    target_industry: String,        // Hedef sektor
    target_geo: String,             // Hedef cografya (orn. "TR", "US")
    sender_name: String,            // Gonderen adi
    sender_email: String,           // Gonderen e-posta
    sender_linkedin: Option<String>,// LinkedIn profili
    target_title_policy: String,    // "ceo_then_founder" veya "ceo_only"
    daily_target: u32,              // Gunluk profil hedefi (1-200)
    daily_send_cap: u32,            // Gunluk gonderim limiti (1-200)
    schedule_hour_local: u8,        // Zamanlama saati (0-23)
    timezone_mode: String,          // "local"
}
```

---

## 10. Potansiyel Musteri Bulma Algoritmasi

### 10.1 Asama 1: Sorgu Plani Olusturma

**Fonksiyon:** `llm_build_lead_query_plan()` / `heuristic_lead_query_plan()`

- Urun tanimi, sektor ve cografyaya dayali arama sorgulari uret
- Anahtar kelime cikarma ve disarida birakma listeleri
- Zaman asimi: 4 saniye (sezgisel yedek)

**Ornek Cikti:**
```json
{
  "queries": [
    "field service management software Turkey",
    "HVAC maintenance companies Istanbul",
    "facility management firms Ankara"
  ],
  "exclude_keywords": ["wikipedia", "linkedin.com", "youtube"]
}
```

### 10.2 Asama 2: Paralel Kesif (3 Akis)

#### A. LLM Birincil Kesfi

**Fonksiyon:** `llm_generate_company_candidates()`

```
Model: gpt-5.3-codex (Codex OAuth uzerinden)
Istem: "B2B satis prospecting icin N adet gercek sirket listele"
Kisitlamalar:
  - KOBi/orta pazar (20-5K calisan)
  - Hedef sektor/cografya
  - Gercek alan adlari (LinkedIn/Wikipedia degil)
Dondurulen: Sirket adi, alan adi, uygunluk nedeni
Maks sirket: 6-12 (daily_target'a gore)
Zaman asimi: 10 saniye
```

#### B. Web Arama Kesfii

**Fonksiyon:** `discover_via_web_search()`

```
Arama motoru: Brave veya yapilandirilmis saglayici
Sorgu sayisi: Asama basina 10
Islem:
  1. Arama sonuclarindan alan adlarini cikar
  2. Anahtar kelimeleri adaylarla esle
  3. Aday puan kartlari olustur
Yedek: Birincil arama saglayicisi basarisiz olursa Brave kullan
Zaman asimi: Grup basina 8 saniye
```

#### C. Turk Dizin Taramasi (Sadece `target_geo == "TR"`)

**Fonksiyon:** `fetch_free_discovery_candidates()`

8 Turk sanayi/dernek dizinini tarar:

| Dizin | URL | Kapsam | Maks Aday |
|-------|-----|--------|-----------|
| **TMB** | tmb.org.tr/en/members | Mekanik muteahhitler | 8 |
| **EUD** | Enerji dernekleri | Enerji sektoru | 6 |
| **ASMUD** | Bakim/onarim | Saha operasyonlari | 10 |
| **PlatformDer** | Sanayi platformu | Genel | 10 |
| **MIB** | Madencilik dernegi | Madencilik | 10 (9 sayfa, calisma basina 3) |
| **IMDER** | Ithalat/ihracat | Ticaret | 8 |
| **ISDER** | ISG dernegi | Saha operasyonlari | 8 |
| **THBB** | Yazi iletisim | Saha operasyonlari | 8 |

**Her tarayici:**
1. Uye listesi HTML'ini indir
2. Sirket adi + web sitesini ayristir
3. Iletisim ipuclarini cikar (isimler, unvanlar, e-postalar)
4. Kaynak atfi ile `FreeDiscoveryCandidate` dondur

### 10.3 Asama 3: Birlestirme ve Tekilsizlestirme

**Fonksiyon:** `merge_all_discovery_sources()`

- LLM, web ve dizin adaylarini birlestir
- Kaynak iletisim ipuclarini birlesimle birlestir
- Alan adina gore tekilsizlestir
- Oncelik puanlamasi uygula

### 10.4 Asama 4: LLM Ilgilik Dogrulamasi

**Fonksiyon:** `llm_validate_candidate_relevance()`

```
Girdi: Ilk 8-40 aday
Islem: Her adayi ICP'ye karsi degerlendir
Cikti: {relevant: bool, confidence: 0.0-1.0, reason: string}

Puanlama:
  Yuksek guven + ilgili:  +15 puan
  Dusuk guven + ilgisiz:  -15 puan

Zaman asimi: 6 saniye
```

### 10.5 Asama 5: Filtreleme ve Siralama

1. Kalite esik degerinin altindaki adaylari kaldir
2. Engellenen alan adlarini kaldir (yaygin SaaS, web3 platformlari vb.)
3. Onceliklendirme:
   - Dogrudan iletisim ipucu (on-tohum gucu)
   - Aday puani
   - Alan adi (alfabetik esitlik kirici)

---

## 11. Zenginlestirme ve Profil Olusturma

### 11.1 Potansiyel Musteri Tohumlama

**Fonksiyon:** `seed_prospect_profiles_for_run()`

- En iyi alan adi adaylarini musteri profillerine donustur
- Iletisim ipuclarini tanimlamak icin sezgisel esleme calistir
- Kesif asamasindan web onbellek verileriyle doldur

### 11.2 Site Tabanli Zenginlestirme

**Fonksiyon:** `enrich_prospect_profiles_with_site_osint()`

```
1. SITE HTML INDIRME: fetch_company_site_html_pages()
   +-- Hedefler: Ana sayfa + Ekip sayfasi + Hakkinda + Kariyer
   +-- Zaman asimi: Site basina 3.5 saniye
   +-- Maks site: 12-24

2. ILETISIM CIKARMA: best_site_contact_enrichment()
   +-- HTML'den isim, unvan, LinkedIn URL'leri ayristir
   +-- CEO/kurucu profilleri, ekip sayfalari ara
   +-- E-posta kaliplarini cikar (@alanadi.com)
   +-- LinkedIn URL'lerini dogrula
   +-- Sinyal puanlamasi

3. SITE HARITASI BAGLANTILARI: fetch_sitemap_enrich_links()
   +-- /sitemap.xml'den ekip/yonetim sayfalarini bul
   +-- Yonetim sayfasi kaliplarini onceliklendir
```

### 11.3 Arama Tabanli Zenginlestirme

**Fonksiyon:** `enrich_prospect_profiles_with_search_osint()`

```
1. SIRKET OSINT SORGULARI:
   +-- "<sirket> CEO"
   +-- "<sirket> kurucu"
   +-- "<sirket> yonetim ekibi"
   +-- "<sirket> linkedin"
   +-- "site:linkedin.com <sirket> <rol>"

2. ARAMA SONUCLARI ISLEME:
   +-- Toplu sorgular (3 esanli, 4 sn zaman asimi)
   +-- Brave Search yedegi
   +-- Parcalardan isim, e-posta, LinkedIn URL cikar
   +-- Sirket LinkedIn URL tespiti

3. AKILLI BAGLANTI ONCELIKLENDIRME:
   +-- Kesfedilen baglantilari varsayilanlarin onunde tut
   +-- Yonetim/ekip/hakkinda yollarini onceliklendir
   +-- Ilgilikle puan ver
```

### 11.4 LLM Tabanli Arastirma

**Fonksiyon:** `llm_enrich_prospect_profiles()`

Musteri profillerini AI uretimli icgorulerle zenginlestirir:

| Alan | Aciklama |
|------|----------|
| `summary` | Sirketin 2-3 cumlelik yonetici ozeti |
| `buyer_roles` | Urunu genellikle satin alan is unvanlari |
| `pain_points` | Sirketin muhtemelen yasadigi sorunlar |
| `trigger_events` | Satin alma hazirligini gosteren olaylar |
| `recommended_channel` | E-posta vs LinkedIn tercihi |
| `outreach_angle` | Kisisellestirmis satis acisi |
| `research_confidence` | 0.0-1.0 guven puani |

**Istem icerikleri:**
- Urun tanimi
- Sirket profil verileri
- Cikarilan iletisim bilgileri
- Eslesen sinyaller
- Hedef sektor/cografya

### 11.5 Sonuc: Musteri Profili Veri Yapisi

```rust
pub struct SalesProspectProfile {
    id: String,
    run_id: String,
    company: String,                   // Sirket adi
    website: String,                   // Web sitesi
    company_domain: String,            // Alan adi
    fit_score: i32,                    // Uygunluk puani
    profile_status: String,            // "contact_ready" veya "company_only"
    summary: String,                   // LLM uretimli ozet
    matched_signals: Vec<String>,      // Neden bu sirket uymakta
    primary_contact_name: Option<String>,
    primary_contact_title: Option<String>,
    primary_email: Option<String>,
    primary_linkedin_url: Option<String>,
    company_linkedin_url: Option<String>,
    osint_links: Vec<String>,          // Kanit baglantilari
    contact_count: u32,                // Bulunan iletisim sayisi
    source_count: u32,                 // Kaynak sayisi
    buyer_roles: Vec<String>,          // CEO, CFO vb.
    pain_points: Vec<String>,          // Tanimlanan sorunlar
    trigger_events: Vec<String>,       // Satis tetikleyicileri
    recommended_channel: String,       // "email" veya "linkedin"
    outreach_angle: String,            // Satis acisi
    research_status: String,           // "llm_enriched" veya "heuristic"
    research_confidence: f32,          // 0.0-1.0
}
```

---

## 12. Lead Donusumu ve Onay Akisi

### 12.1 Lead Olusturma

```
Musteri Profili (contact_ready)
    |
    +-- Sirket bilgilerini topla
    +-- En iyi birincil iletisimi sec (en yuksek puan)
    +-- Kisisellestirmis mesaj taslaklari olustur:
    |     +-- E-posta konu satiri
    |     +-- E-posta govdesi (aci nokta odakli)
    |     +-- LinkedIn mesaji (kisisellestirme + CTA)
    |
    v
  SalesLead
```

### 12.2 Lead Veri Yapisi

```rust
pub struct SalesLead {
    id: String,
    company: String,
    website: String,
    contact_name: String,
    contact_title: String,
    linkedin_url: Option<String>,
    email: Option<String>,
    reasons: Vec<String>,           // Neden bu kisi uygun
    email_subject: String,          // Taslak konu satiri
    email_body: String,             // Taslak e-posta govdesi
    linkedin_message: String,       // Taslak LinkedIn mesaji
    score: i32,
    status: String,                 // "draft", "pending_approval" vb.
}
```

### 12.3 Onay Akisi

```
Lead Olusturuldu
    |
    v
queue_approvals_for_lead()
    |
    +-- Her lead/kanal kombinasyonu icin onay kaydi olustur
    +-- JSON yukunu depola (e-posta/LinkedIn detaylari)
    +-- Durumu "pending" olarak ayarla
    |
    v
KULLANICI PANELDE INCELER
    |
    +-- "Onayla" --> approve_and_send()
    |     +-- E-posta: SMTP ile gonder
    |     +-- LinkedIn: Tarayici otomasyonu ile gonder
    |     +-- Teslimat kaydi olustur
    |
    +-- "Reddet" --> reject_sales_approval()
          +-- Durumu "rejected" olarak guncelle
```

### 12.4 Teslimat Takibi

```rust
pub struct SalesDelivery {
    id: String,
    channel: String,       // "email" veya "linkedin"
    recipient: String,
    status: String,        // "sent" veya "failed"
    error: Option<String>,
    timestamp: String,
}
```

---

## 13. Dashboard ve Ekranlar

**Dosyalar:**
- `crates/openfang-api/static/index_body.html` (~27KB)
- `crates/openfang-api/static/js/pages/sales.js` (~611 satir)
- `crates/openfang-api/static/js/api.js` (~310 satir)
- `crates/openfang-api/static/css/` (tema, duzen, bilesenler)

### 13.1 Teknik Altyapi

- **Framework:** Alpine.js (hafif reaktif framework)
- **Montaj:** Derleme zamani HTML/CSS/JS birlestirme (`webchat.rs`)
- **Tek Sayfa:** Yonlendirme yok, tum icerik `/` yolunda
- **Paketlenmis:** Tum kutuphane dosyalari gomulu (CDN bagimliligi yok)
- **Tema:** Acik / Karanlik / Sistem (OS tercihini takip eder)
- **Bildirim:** Toast sistemi (basari/hata/uyari/bilgi)

### 13.2 Baslangic Paneli (Onboarding) - 4 Adim

#### Adim 1: Codex OAuth Kurulumu
```
+--------------------------------------------------+
| CODEX OAUTH BAGLA                                |
|                                                   |
| [Bagli] veya [Baglanti bekliyor]                 |
|                                                   |
| "~/.codex/auth.json dosyasindan iceri aktar"     |
| [Iceri Aktar] [Cikis Yap]                        |
|                                                   |
| Manuel OAuth kodu giris yedegi                    |
+--------------------------------------------------+
```

#### Adim 2: Sirket Ozeti
```
+--------------------------------------------------+
| SIRKET OZETI                                      |
|                                                   |
| [Metin alani: sirket/urun/ICP tanimi]            |
|                                                   |
| [Brieften otomatik doldur]                       |
| --> /api/sales/onboarding/brief'e gonderir       |
+--------------------------------------------------+
```

#### Adim 3: Profil Dogrulama
```
+--------------------------------------------------+
| PROFIL DOGRULAMA                                  |
|                                                   |
| URUN:                                             |
|   Urun Adi: [___________]                        |
|   Urun Tanimi: [___________]                     |
|   Hedef Sektor: [___________]                    |
|   Hedef Cografya: [___________]                  |
|   Deger Onerisi: [___________]                   |
|                                                   |
| GONDEREN:                                         |
|   Ad Soyad: [___________]                        |
|   E-posta: [___________]                         |
|   LinkedIn: [___________] (opsiyonel)            |
|                                                   |
| YAPILANDIRMA:                                     |
|   Unvan Politikasi: [CEO sonra Kurucu / Sadece CEO]|
|   Gunluk Profil Hedefi: [1-200]                  |
|   Gunluk Gonderim Limiti: [1-200]                |
|   Zamanlama Saati: [0-23]                        |
|                                                   |
| [Profili Kaydet]                                  |
+--------------------------------------------------+
```

#### Adim 4: Ilk Prospecting Calismasi
```
+--------------------------------------------------+
| ILK CALISMAYI BASLAT                             |
|                                                   |
| [Ilk Run'i Baslat]                               |
|                                                   |
| Sonuc Tablosu:                                    |
| Sirket | Durum | Birincil Kisi | Kanal | Uyum | ICP|
| -------|-------|-------------- |-------|------|----|
| ABC    | ready | Ali V. CEO   | email | 85   | .. |
| XYZ    | only  | -            | -     | 72   | .. |
+--------------------------------------------------+
```

### 13.3 Operasyon Paneli (Ana Ekran)

#### Ust Bolum: 6 Istatistik Karti
```
+--------+--------+--------+--------+--------+--------+
| Toplam | Profil | Hazir  | Firma  | Onay   | Teslim |
| Calis. | Sayisi | Kisi   | Bekl.  | Bekl.  | Sayisi |
|  12    |  87    |  45    |  42    |   3    |  28    |
+--------+--------+--------+--------+--------+--------+
                                        ^^^
                                    (>0 ise vurgulu)
```

#### A. Calisma Gecmisi
```
+--------------------------------------------------+
| CALISMA GECMISI                        [Yeni Run]|
|                                                   |
| Baslangic | Durum    | Profil | Hazir | Onay | Not|
| 15:32     | tamaml.  |   15   |   8   |   3  | .. |
| 09:00     | tamaml.  |   22   |  12   |   5  | .. |
| Dun 09:00 | basariz. |    0   |   0   |   0  | er |
|           |          |        |       |      |    |
| [Profilleri Ac]                                   |
+--------------------------------------------------+
```

#### B. Musteri Profilleri (Maks 200)
```
+------------------------------------------------------------------+
| MUSTERI PROFILLERI                                                 |
|                                                                    |
| Sirket (ad+alan+ozet) | Durum       | Kisi      | Kanal | Uyum  |
| ABC Ltd (abc.com)     | [yesil]hzr  | Ali V.    | email |  85   |
|   "Saha servis coz."  |             | CEO       |       |       |
| XYZ AS (xyz.com.tr)   | [sari]firma | -         | -     |  72   |
|   "Enerji muhendis."  |             |           |       |       |
|                                                                    |
| [Profili Ac] --> Dosya acilir                                     |
+------------------------------------------------------------------+
```

#### C. Secili Hesap Dosyasi (Dossier)
```
+------------------------------------------------------------------+
| HESAP DOSYASI: ABC Ltd                                            |
|                                                                    |
| Ozet: "ABC Ltd, Istanbul merkezli saha servis yonetimi..."       |
|                                                                    |
| Arastirma Durumu: [LLM Zenginlestirmis] %87 Guven               |
|                                                                    |
| Birincil Iletisim: Ali Vural - CEO                               |
| Onerilen Kanal: E-posta | Kisi Sayisi: 3 | Sinyal Kaynagi: 4    |
| Sonraki Adim Onerisi: "Ilk iletisim e-posta ile..."             |
|                                                                    |
| Alici Rolleri: [CEO] [CTO] [Operasyon Muduru]                   |
| Erisim Acisi: "Saha operasyonlarinda dijital donusum..."         |
| Aci Noktalari: [Kagit bazli is emirleri] [Geciken raporlar]     |
| Tetikleyici Olaylar: [Yeni sube acilisi] [ISO sertifikasyonu]  |
| OSINT Baglantilari: [linkedin.com/...] [abc.com/team]           |
+------------------------------------------------------------------+
```

#### D. Mesaj Onay Kuyrugu
```
+------------------------------------------------------------------+
| MESAJ ONAY KUYRUGU                                                |
|                                                                    |
| Kanal    | Alici        | Taslak Mesaj | Durum   | Islem          |
| E-posta  | ali@abc.com  | [genislet]   | bekl.   | [Onayla][Red]  |
| LinkedIn | /in/alivural | [genislet]   | bekl.   | [Onayla][Red]  |
| E-posta  | m@xyz.com    | [genislet]   | onayli  | -              |
|                                                                    |
| Taslak Onizleme (katlanabilir):                                  |
| +--------------------------------------------------------------+ |
| | Konu: Saha Servis Dijitallesme Firsati                       | |
| |                                                               | |
| | Sayin Ali Bey,                                                | |
| | ABC Ltd'nin saha operasyonlarinda kagit bazli...             | |
| +--------------------------------------------------------------+ |
+------------------------------------------------------------------+
```

#### E. Ham Lead Adaylari
```
+------------------------------------------------------------------+
| HAM LEAD ADAYLARI                                                 |
|                                                                    |
| Sirket (ad+alan) | Kisi     | E-posta      | LinkedIn | Puan | ICP|
| ABC (abc.com)    | Ali V.   | ali@abc.com  | /in/ali  |  85  | .. |
| XYZ (xyz.com.tr) | Mehmet K.| m@xyz.com.tr | /in/mk   |  72  | .. |
+------------------------------------------------------------------+
```

#### F. Teslimat Kaydi
```
+--------------------------------------------------+
| TESLIMAT KAYDI                                    |
|                                                   |
| Kanal    | Alici       | Durum  | Zaman  | Hata  |
| E-posta  | ali@abc.com | [ysl]  | 15:45  | -     |
| LinkedIn | /in/alivural| [krm]  | 15:46  | timeout|
+--------------------------------------------------+
```

### 13.4 JavaScript Veri Modeli

```javascript
// Ana Uygulama Durumu
{
  page: 'prospecting',
  themeMode: 'system|light|dark',
  connected: true/false,
  wsConnected: true/false,
}

// Satis Sayfasi Durumu
{
  profile: { /* SalesProfile alanlari */ },
  onboarding: {
    completed: false,
    active_step: 1,
    steps: [{key, title, done}, ...],
    oauth_connected: false,
    has_brief: false,
    profile_ready: false,
    first_run_ready: false,
  },
  oauth: {
    connected: false,
    source: '',
    auth_url: '',
    state: ''
  },
  runs: [],          // Calisma gecmisi
  prospects: [],     // Musteri profilleri
  leads: [],         // Ham leadler
  approvals: [],     // Onay kuyrugu
  deliveries: [],    // Teslimat kaydi
}
```

---

## 14. Codex OAuth Entegrasyonu

**Dosya:** `crates/openfang-api/src/codex_oauth.rs` (~1335 satir)

### 14.1 Ne Ise Yarar?

Satis motoru, yuksek kaliteli sirket kesfii ve profilleme icin `gpt-5.3-codex`
modelini kullanir. Bu model, OpenAI Codex OAuth araciligiyla erisim gerektirir.

### 14.2 OAuth Akisi

```
1. Kullanici "OAuth Bagla" tiklar
2. /api/auth/codex/start cagirilir
3. PKCE akisi baslatilir (auth.openai.com)
4. Kullanici yetkilendirir
5. /api/auth/codex/callback geri cagirim alir
6. Erisim tokeni -> ChatGPT hesap ID cikarimi (JWT)
7. Token yerel olarak saklanir
8. Yenileme tokeni otomatik kullanilir
```

### 14.3 Alternatif Yontemler

- **CLI Iceri Aktarma:** `~/.codex/auth.json` dosyasindan tokeni al
- **Manuel Kod Giris:** OAuth kod'unu elle yapistir
- **Durum Kontrolu:** `/api/auth/codex/status` ile baglanti durumu sorgula

### 14.4 Model Yapilandirmasi

```
Model: gpt-5.3-codex
Saglayici: openai-codex (ozel LLM surucu)
Token Limitleri:
  - Sirket uretimi: 2400 token
  - Ilgilik dogrulamasi: 1400 token
  - Profil zenginlestirme: degisken
Sicaklik: 0.0-0.2 (belirleyici)
Dusunme Gayreti: Medium
```

---

## 15. A2A ve OFP Ag Protokolleri

### 15.1 OFP (OpenFang Protocol) - Mesh Aglama

**Dosya:** `crates/openfang-wire/src/`

**Amac:** Makineler arasi agent kesfii ve iletisim

**Protokol:**
- TCP baglantilari + JSON-RPC cerceveleme
- 4 bayt buyuk-endian uzunluk basligi + JSON govde (maks 16MB)
- HMAC-SHA256 kimlik dogrulama (zorunlu)

**Mesaj Turleri:**
- `Handshake` / `HandshakeAck`: Karsilikli kimlik dogrulama
- `Discover` / `DiscoverResult`: Agent arama
- `AgentMessage` / `AgentResponse`: Agent iletisimi
- `Ping` / `Pong`: Canlilik kontrolu
- `AgentSpawned` / `AgentTerminated` / `ShuttingDown`: Bildirimler

**Peer Registry:**
- Bagli eslerin thread-safe takibi
- Her es icin: node_id, node_name, adres, agentlar, durum
- `find_agents(query)`: Tum bagli eslerdeki agentlari ara

### 15.2 A2A (Agent-to-Agent) Protokolu

**Dosya:** `crates/openfang-runtime/src/a2a.rs`

**Amac:** Framework'ler arasi agent birlikte calisabilirligi (Google A2A)

**Agent Karti:** `/.well-known/agent.json` adresinde sunulur
```json
{
  "name": "coder",
  "description": "Kod yazan agent",
  "url": "http://127.0.0.1:4200",
  "capabilities": {
    "streaming": true,
    "state_transition_history": true
  },
  "skills": [...]
}
```

**Gorev Yasam Dongusu:**
```
Submitted -> Working -> Completed
                     -> Failed
                     -> Cancelled
                     -> InputRequired
```

### 15.3 Ag Topolojisi

```
OpenFang Kernel A              OpenFang Kernel B
+-- PeerNode (dinle)     <-->  +-- PeerNode (dinle)
|   +-- TCP Sunucu:9090       |   +-- TCP Sunucu:9091
|   +-- Agentlar: [A1, A2]   |   +-- Agentlar: [B1, B2]
+-- PeerRegistry              +-- PeerRegistry
    +-- Esler: [B]                +-- Esler: [A]

Harici A2A Agent X
+-- /.well-known/agent.json
+-- /a2a endpoint
    ^
    | (baslatmada kesfedilir)
    |
OpenFang Kernel A
+-- a2a_external_agents: {url -> AgentCard}
```

---

## 16. Butce ve Maliyet Takibi

### 16.1 Uc Katmanli Mimari

**1. Model Katalogu ve Fiyatlandirma** (`model_catalog.rs`)
- 130+ dahili model, 28 saglayici
- Her model: `input_cost_per_m` ve `output_cost_per_m`

**2. Kullanim Kaydi** (`usage.rs`)
```sql
CREATE TABLE usage_events (
  id TEXT PRIMARY KEY,
  agent_id TEXT,
  timestamp TEXT,
  model TEXT,
  input_tokens INTEGER,
  output_tokens INTEGER,
  cost_usd REAL,
  tool_calls INTEGER
);
```

**3. Olcum/Uygulama** (`metering.rs`)
```rust
pub struct ResourceQuota {
    max_cost_per_hour_usd: f64,
    max_cost_per_day_usd: f64,
    max_cost_per_month_usd: f64,
    max_llm_tokens_per_hour: u64,
    max_tool_calls_per_minute: u32,
}
```

### 16.2 Maliyet Hesaplama Akisi

```
LLM cagrisi tamamlandi (input/output token sayilari)
    |
    v
Model fiyatlandirmasi katalogdan alinir
    |
    v
Maliyet = (input_tokens / 1M * input_cost) + (output_tokens / 1M * output_cost)
    |
    v
UsageRecord olusturulur ve SQLite'a yazilir
    |
    v
Sonraki istekte olcum motoru kotayi kontrol eder
```

### 16.3 Sorgu Yontemleri

- `query_hourly(agent_id)` - Son saat
- `query_daily(agent_id)` - Bugun
- `query_monthly(agent_id)` - Mevcut takvim ayi
- `query_global_hourly/monthly()` - Tum agentlar
- `query_by_model()` - Modele gore toplu
- `query_daily_breakdown(days)` - Zaman serisi

---

## 17. Workflow - Is Akisi Motoru

**Dosya:** `crates/openfang-kernel/src/workflow.rs` (~1367 satir)

### 17.1 Tanim

```rust
pub struct Workflow {
    steps: Vec<WorkflowStep>,
}

pub struct WorkflowStep {
    name: String,
    agent: StepAgent,              // Kimlige veya isme gore
    prompt_template: String,       // {{input}}, {{degisken_adi}}
    mode: StepMode,                // Sequential, FanOut, Collect, vb.
    timeout_secs: u64,
    error_mode: ErrorMode,         // Fail, Skip, Retry
    output_var: Option<String>,    // Sonucu sonraki adimlar icin sakla
}
```

### 17.2 Calistirma Modlari

| Mod | Aciklama |
|-----|----------|
| **Sequential** | Her adim oncekinin tamamlanmasini bekler |
| **FanOut** | Birden fazla adim paralel calisir |
| **Collect** | FanOut sonuclarini toplar |
| **Conditional** | Onceki cikti kosulu icermiyorsa atla |
| **Loop** | "Until" dizesi bulunana veya maks iterasyona kadar tekrarla |

### 17.3 Hata Isleme

- **Fail**: Is akisini durdur (varsayilan)
- **Skip**: Hatada devam et, adim None dondur
- **Retry**: N yeniden denemeye kadar ustel geri cekilme

---

## 18. Zamanlama ve Otomasyon

### 18.1 Cron Zamanlayici

**Dosya:** `crates/openfang-kernel/src/cron.rs` (~693 satir)

```rust
pub struct CronJob {
    id: CronJobId,
    agent_id: AgentId,
    schedule: CronSchedule,   // "0 9 * * *" (cron sozdizimi)
    prompt: String,            // Agent'a gonderilecek mesaj
    enabled: bool,
    next_run: DateTime<Utc>,
}
```

- Gorevler JSON olarak `~/.openfang/data/cron_jobs.json` dosyasina kaydedilir
- Atomik yazma (gecici dosyaya yaz, sonra yeniden adlandir)
- 5 ardisik hatadan sonra otomatik devre disi birakma

### 18.2 Arka Plan Calistirici

**Dosya:** `crates/openfang-kernel/src/background.rs` (~457 satir)

**Zamanlama Modlari:**
- **Reactive**: Otonom davranis yok
- **Continuous**: Her N saniyede bir kendi kendini tetikler
- **Periodic**: Basitlestirilmis cron benzeri zamanlama
- **Proactive**: Eslesen tetikleyici olaylarinda uyanir

**Esanlilik Limiti:** Global semafor, arka plan LLM cagrilarini 5 ile sinirlar.

---

## 19. Kanal Adapterleri - 40 Platform

**Dosya:** `crates/openfang-channels/src/`

### Desteklenen Platformlar

**Dalga 1 (Temel):**
Discord, Slack, Telegram, Teams, WhatsApp, Signal, Twitch, Matrix, IRC,
Mattermost, Rocketchat, Google Chat, XMPP, Zulip

**Dalga 2-5 (Genisletilmis):**
Bluesky, Feishu, Line, Mastodon, Messenger, Reddit, Revolt, Viber,
Flock, Guilded, Keybase, Nextcloud, Nostr, Pumble, Threema, Twist, WebEx,
DingTalk, Discourse, Gitter, Gotify, LinkedIn, Mumble, Ntfy, Webhook,
E-posta (SMTP/IMAP)

### Kanal Basi Yapilandirma

```rust
pub struct ChannelOverrides {
    model: Option<String>,           // Kanal-ozel model
    system_prompt: Option<String>,   // Kanal-ozel istem
    dm_policy: DmPolicy,            // Respond, AllowedOnly, Ignore
    group_policy: GroupPolicy,       // All, MentionOnly, CommandsOnly, Ignore
    rate_limit_per_user: u32,        // Dakika basina mesaj
    output_format: OutputFormat,     // Markdown, TelegramHtml, SlackMrkdwn, PlainText
}
```

---

## 20. Guvenlik Mimarisi

### 20.1 Kimlik Dogrulama

- **Bearer Token:** `Authorization: Bearer <token>`
- **Sorgu Parametresi:** `?token=<token>` (WebSocket/SSE icin)
- **Sabit zamanli karsilastirma:** Zamanlama saldirilarina karsi koruma
- **Geri dongu modu:** API anahtari yoksa sadece yerel baglantilar izinli

### 20.2 Yetki Sistemi

- Manifest yetki talepleri bildirir
- Kernel kurallara gore yetki verir
- Arac calistirma izin listesini kontrol eder
- Ebeveyn -> cocuk yetki mirasii

### 20.3 Onay Sistemi

- Yuksek riskli araclar insan onayi gerektirir
- Onayana kadar calistirma engellenir
- Zaman asimi yedegi

### 20.4 Leke Takibi

- Verileri etiketler: ExternalNetwork, Secret
- Havuzlari kontrol eder: shell_exec, net_fetch
- Veri sizintisini onler

### 20.5 Denetim Kaydi

- Merkle hash zinciri (SHA-256)
- Kaydedilenler: AgentSpawn, AgentMessage, AgentKill
- Uyumluluk icin degistirilemez gecmis

### 20.6 Diger Guvenlik Onlemleri

- HMAC-SHA256 es kimlik dogrulamasi (OFP)
- AES-256-GCM sifrelenmis kimlik kasasi
- OAuth2 PKCE akisi
- SSRF korumasi (web_fetch)
- CORS politikasi
- Hiz sinirlamasi (GCRA algoritmasi)
- Guvenlik basliklari (X-Content-Type-Options, X-Frame-Options, X-XSS-Protection)

---

## 21. Yapilandirma Sistemi

**Dosya:** `crates/openfang-types/src/config.rs` (~3554 satir)

### Ana Yapilandirma Dosyasi: `~/.openfang/config.toml`

```toml
[default_model]
provider = "anthropic"
model = "claude-sonnet-4-20250514"
api_key_env = "ANTHROPIC_API_KEY"

[memory]
decay_rate = 0.05

[network]
listen_addr = "127.0.0.1:4200"
shared_secret = "gizli-anahtar"
bootstrap_peers = ["192.168.1.100:9090"]

[budget]
max_hourly_usd = 10.0
max_daily_usd = 100.0
max_monthly_usd = 1000.0
alert_threshold = 0.8

[web]
search_provider = "auto"     # brave, tavily, perplexity, duckduckgo
cache_ttl_minutes = 15

[browser]
headless = true
timeout_secs = 30

[[mcp_servers]]
name = "filesystem"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]

[telegram]
bot_token_env = "TELEGRAM_BOT_TOKEN"
```

### Yapilandirma Bilesimi

```toml
include = ["base.toml", "overrides.toml"]
```
- Onceki icerikler sonrakiler tarafindan gecirilir
- Kok yapilandirma tum icerikleri gecer
- Guvenlik: Mutlak yollar, `..` gecisi, dairesel referanslar reddedilir
- Maks icleme derinligi: 10 seviye

### Sicak Yeniden Yukleme

- Her 30 saniyede `config.toml` kontrol edilir
- Degisiklik algilanirsa yeniden baslatmadan yukler
- Modlar: off, restart, hot, hybrid

---

## 22. API Endpoint Katalogu

### Temel Endpoint Gruplari

| Grup | Endpoint Sayisi | Aciklama |
|------|----------------|----------|
| Agent Yonetimi | ~20 | Olusturma, listeleme, silme, mod degistirme |
| Agent Iletisimi | ~5 | Mesaj gonderme, akis, WebSocket |
| Agent Oturum | ~6 | Oturum olustur/sifirla/degistir/sikistir |
| Beceri ve Arac | ~7 | Beceri listele/yukle/kaldir |
| Kanal | ~10 | Kanal yapilandir/test et/yukle |
| Dosya Yonetimi | ~7 | Agent dosyalari/yuklemeler |
| Bellek ve Bilgi | ~3 | KV deposu, semantik arama |
| Tetikleyici | ~4 | Tetikleyici olustur/listele/guncelle/sil |
| Zamanlama ve Is Akisi | ~8 | Cron isler, is akislari |
| Hands | ~9 | Otonom paket agentlar |
| MCP | ~3 | MCP sunuculari |
| **Satis Motoru** | **~15** | **Profil, kesif, onay, teslimat** |
| Model ve Saglayici | ~6 | Model listele, saglayici test et |
| Onay | ~5 | Onay olustur/listele/onayla/reddet |
| A2A Protokolu | ~8 | Agent karti, gorev gonder/sorgula |
| Entegrasyon | ~6 | Entegrasyon ekle/kaldir/yeniden bagla |
| Cihaz Eslestirme | ~5 | Eslestirme akisi, cihaz yonetimi |
| Cron Is | ~4 | Cron is yonetimi |
| Kullanim ve Butce | ~7 | Kullanim istatistikleri, butce durumu |
| Denetim ve Guvenlik | ~5 | Denetim kaydi, guvenlik durumu |
| Sistem | ~11 | Saglik, metrikler, kapatma |
| Yapilandirma | ~4 | Yapilandirma oku/yaz/sema |
| Goc | ~3 | Goc algilama/tarama/calistirma |
| Webhook | ~2 | Harici tetikleyici |
| OpenAI Uyumu | ~2 | /v1/chat/completions, /v1/models |
| UI | ~4 | Dashboard, logo, favicon |

### Satis Motoru Endpoint'leri (Detay)

| Yontem | Yol | Islem |
|--------|-----|-------|
| GET | `/api/sales/profile` | Satis profilini getir |
| PUT | `/api/sales/profile` | Satis profilini guncelle |
| POST | `/api/sales/profile/autofill` | Brieften otomatik doldur |
| GET | `/api/sales/onboarding/status` | Baslangic ilerleme durumu |
| POST | `/api/sales/onboarding/brief` | Sirket briefini kaydet |
| POST | `/api/sales/run` | Kesif calismasi baslat |
| GET | `/api/sales/runs` | Calisma gecmisini listele |
| GET | `/api/sales/prospects` | Musteri profillerini listele |
| GET | `/api/sales/leads` | Leadleri listele |
| GET | `/api/sales/approvals` | Onay kuyugunu listele |
| POST | `/api/sales/approvals/{id}/approve` | Onayla ve gonder |
| POST | `/api/sales/approvals/{id}/reject` | Reddet |
| GET | `/api/sales/deliveries` | Teslimat kayitlarini listele |
| POST | `/api/auth/codex/start` | Codex OAuth akisini baslat |
| GET | `/api/auth/codex/status` | OAuth durumunu kontrol et |

---

## 23. Tablo: Temel Istatistikler

### Satis Motoru Esik Degerleri

| Sabit | Deger | Aciklama |
|-------|-------|----------|
| `MIN_DOMAIN_RELEVANCE_SCORE` | 5 | Minimum uygun aday puani |
| `MAX_DISCOVERY_QUERIES` | 10 | Asama basina web arama sorgusu |
| `MAX_DIRECT_ENRICH_ATTEMPTS` | 12 | Site HTML indirmeleri |
| `MAX_WEB_CONTACT_SEARCH_ATTEMPTS` | 12 | OSINT arama sorgulari |
| `DIRECT_ENRICH_TIMEOUT_MS` | 3500 | Site indirme zaman asimi |
| `SITE_PAGE_FETCH_TIMEOUT_MS` | 1600 | Sayfa indirme zaman asimi |
| `MAX_OSINT_LINKS_PER_PROSPECT` | 6 | Profil basi kanit baglantisi |
| `MAX_OSINT_SEARCH_TARGETS` | 24 | Zenginlestirme basi hedef |
| `LLM_COMPANY_GENERATION_TIMEOUT_SECS` | 10 | Sirket uretim zaman asimi |
| `LLM_RELEVANCE_VALIDATION_TIMEOUT_SECS` | 6 | Dogrulama zaman asimi |
| `SALES_RUN_REQUEST_TIMEOUT_SECS` | 240 | Toplam calisma zaman asimi |

### Veritabani Tablolari

| Tablo | Veritabani | Amac |
|-------|-----------|-------|
| `agents` | openfang.db | Kalici agent kayitlari |
| `sessions` | openfang.db | Konusma gecmisleri |
| `memories` | openfang.db | Semantik bellek kayitlari |
| `entities` | openfang.db | Bilgi grafi varliklari |
| `relations` | openfang.db | Bilgi grafi iliskileri |
| `kv_store` | openfang.db | Anahtar-deger deposu |
| `task_queue` | openfang.db | Gorev kuyrugu |
| `usage_events` | openfang.db | LLM kullanim kayitlari |
| `sales_profile` | sales.db | Satis profili |
| `sales_runs` | sales.db | Calisma gecmisi |
| `leads` | sales.db | Uretilen leadler |
| `approvals` | sales.db | Onay kuyrugu |
| `deliveries` | sales.db | Teslimat kayitlari |
| `prospect_profiles` | sales.db | Musteri profilleri |
| `discovered_domains` | sales.db | Tekilsizlestirme |

### Dosya Buyukluk Haritasi

| Dosya | Satir | Gorev |
|-------|-------|-------|
| `sales.rs` | ~14.148 | Satis motoru tam kodu |
| `kernel.rs` | ~5.000 | Cekirdek orkestrasyon |
| `routes.rs` | ~8.698 | HTTP API endpoint'leri |
| `config.rs` (types) | ~3.554 | Yapilandirma tipleri |
| `model_catalog.rs` | ~2.574 | 130+ model katalogu |
| `agent_loop.rs` | ~1.000+ | Cekirdek LLM dongusu |
| `tool_runner.rs` | ~1.500 | Arac calistirma + guvenlik |
| `workflow.rs` | ~1.367 | Is akisi motoru |
| `codex_oauth.rs` | ~1.335 | OAuth entegrasyonu |
| `metering.rs` | ~692 | Maliyet olcumu |
| `cron.rs` | ~693 | Cron zamanlayici |

---

## Sonuc

OpenFang, su bilesenlerden olusan kapsamli bir Agent Isletim Sistemidir:

1. **Kernel**: Tum agentlarin yasam dongusunu yoneten merkezi islem yoneticisi
2. **Agent Loop**: LLM -> Arac -> Sonuc donguleri ile guvenlik korumalari
3. **Satis Motoru**: 5 asamali paralel kesif + zenginlestirme + profilleme + otomatik mesajlasma
4. **Bellek**: Semantik arama + bilgi grafi + KV deposu + oturum yonetimi
5. **Ag**: HMAC dogrulamali mesh ag (OFP) + A2A birlikte calisabilirlik
6. **Butce**: Per-agent ve global maliyet kontrolleri + gercek zamanli uygulama
7. **Is Akisi**: Sirali/paralel cok agentli boru hatlari
8. **Otomasyon**: Cron isleri + arka plan calistirici + olay tetikleyicileri
9. **Kanallar**: 40 mesajlasma platformu entegrasyonu
10. **Guvenlik**: Yetki, onay, leke takibi, denetim, sifreleme, HMAC

Sistem, musteri bulma surecinde web arama, dizin tarama, LLM arastirma ve OSINT
zenginlestirme yontemlerini paralel olarak kullanarak yuksek kaliteli B2B prospect
profilleri olusturur. Bulunan profiller, insan onay akisindan gecirilerek e-posta
veya LinkedIn uzerinden kisisellestirmis mesajlarla iletisime gecirilir.


===============================================================
# PARCA 2 — PROSPECTING ENGINE TEKNIK REFERANS
===============================================================

# OpenFang Prospecting Engine — Eksiksiz Sistem Referansi

> Bu dokuman, OpenFang B2B prospecting motorunun her detayini kapsar.
> Amac: Bu dokumani okuyan herhangi bir LLM (GPT, Gemini, Claude) veya
> insan gelistirici, uygulamayi hic gormeden sistemi tamamen anlayabilmeli.
>
> Icindekiler: Mevcut kodun birebir analizi, canli verideki 12 bug,
> gercek Rust struct'lari, gercek fonksiyon kodlari, gercek LLM prompt'lari,
> gercek HTML/JS kaynak kodu, gercek SQL semalari, ekran tasarimlari,
> hedef mimari ve uygulama yol haritasi.

---

## ICINDEKILER

**BOLUM I — TEKNIK ENVANTER**
- 1.1 Dosya Haritasi
- 1.2 Tum Sabitler (38 adet, gercek kod)
- 1.3 Tum Veri Yapilari (8 struct, gercek Rust kodu)
- 1.4 Veritabani Semasi (8 tablo, gercek SQL)
- 1.5 API Endpoint Listesi (14 handler, satir numaralari)

**BOLUM II — PIPELINE MEKANIZMASI**
- 2.1 Pipeline Genel Akis (7 asama)
- 2.2 Asama 1: Sorgu Planlama (gercek LLM prompt)
- 2.3 Asama 2: Paralel Kesif (3 kanal, gercek kod akisi)
- 2.4 Asama 3: Birlestirme ve Tekilsizlestirme
- 2.5 Asama 4: LLM Ilgilik Dogrulamasi (gercek prompt)
- 2.6 Asama 5: Filtreleme ve Siralama
- 2.7 Asama 6: Prospect Profil Tohumlama + LLM Zenginlestirme
- 2.8 Asama 7: Lead Uretim Dongusu (gercek kod akisi)

**BOLUM III — TURK DIZIN TARAYICILARI**
- 3.1 Orchestrator: fetch_free_discovery_candidates()
- 3.2 TMB Tarayici (URL, regex, cikarilan veriler)
- 3.3 EUD Tarayici
- 3.4 ASMUD Tarayici
- 3.5 Platformder Tarayici
- 3.6 MIB Tarayici (sayfa rotasyonu)
- 3.7 IMDER Tarayici (detay sayfalari)
- 3.8 ISDER Tarayici
- 3.9 THBB Tarayici

**BOLUM IV — FILTRELEME VE PUANLAMA**
- 4.1 lead_score() fonksiyonu (gercek kod)
- 4.2 prospect_status() fonksiyonu (gercek kod)
- 4.3 email_is_actionable_outreach_email() (gercek kod)
- 4.4 email_is_generic_role_mailbox() (gercek kod)
- 4.5 is_consumer_email_domain() (gercek kod + tam liste)
- 4.6 is_blocked_company_domain() (gercek kod + tam liste)
- 4.7 contact_name_is_placeholder() (gercek kod + tam liste)
- 4.8 geo_is_turkey(), profile_targets_field_ops(), profile_targets_energy()

**BOLUM V — MESAJ URETIMI VE TESLIMAT**
- 5.1 E-posta Konu Satiri (gercek sablon kodu)
- 5.2 E-posta Govdesi (gercek sablon kodu, TR + EN)
- 5.3 LinkedIn Mesaji (gercek sablon kodu)
- 5.4 ICP Sinyal Uretimi (build_sales_lead_reasons)
- 5.5 Pain Point Uretimi (build_prospect_pain_points)
- 5.6 Trigger Event Uretimi (build_prospect_trigger_events)
- 5.7 Outreach Angle Uretimi (build_prospect_outreach_angle)
- 5.8 Prospect Ozet Uretimi (build_prospect_summary)
- 5.9 send_email() — SMTP kodu
- 5.10 send_linkedin() — Tarayici otomasyon kodu
- 5.11 Onay Akisi (approve_and_send, reject_approval, queue_approvals_for_lead)

**BOLUM VI — LLM ENTEGRASYONU**
- 6.1 LLM Yapilandirmasi (model, provider, sabitler)
- 6.2 llm_build_lead_query_plan() — Tam Prompt
- 6.3 llm_generate_company_candidates() — Tam Prompt
- 6.4 llm_validate_candidate_relevance() — Tam Prompt
- 6.5 llm_enrich_prospect_profiles() — Tam Prompt
- 6.6 llm_autofill_profile() — Tam Prompt

**BOLUM VII — ZAMANLAMA VE OTOMASYON**
- 7.1 spawn_sales_scheduler() (gercek kod)
- 7.2 Gunluk Cap Yonetimi (deliveries_today, already_ran_today)
- 7.3 Onboarding Akisi (build_onboarding_status, 4 adim)

**BOLUM VIII — KULLANICI ARAYUZU**
- 8.1 HTML Yapisi (index_body.html, 508 satir, tam analiz)
- 8.2 JavaScript Veri Modeli (sales.js, 612 satir, tam analiz)
- 8.3 API Client (api.js)
- 8.4 Onboarding Ekrani (4 adim, HTML yapisi)
- 8.5 Operasyon Paneli (istatistik kartlari, tablolar)
- 8.6 Prospect Profil Detay (dossier gorunumu)
- 8.7 Mesaj Onay Kuyrugu (onayla/reddet akisi)
- 8.8 Ham Lead Tablosu
- 8.9 Teslimat Kaydi
- 8.10 Tema Sistemi ve Bildirimler

**BOLUM IX — CANLI VERIDE TESPIT EDILEN 12 BUG**
- 9.1-9.12 Her bug icin: belirti, kok neden, satir numarasi, cozum

**BOLUM X — HEDEF MIMARI VE YOL HARITASI**
- 10.1 Hedef Pipeline (7 asama, iyilestirmeler)
- 10.2 Yeni Puanlama Sistemi (0-1000)
- 10.3 LLM Mesaj Uretimi Tasarimi
- 10.4 Yeni Ekran Tasarimlari
- 10.5 Uygulama Yol Haritasi → bkz Parca 4, Bolum 30

---

# BOLUM I — TEKNIK ENVANTER

## 1.1 Dosya Haritasi

```
crates/openfang-api/
  src/
    sales.rs              14.466 satir   Satis motoru tam kodu
    codex_oauth.rs         1.335 satir   Codex OAuth entegrasyonu
    server.rs                942 satir   HTTP sunucusu (satir 751: scheduler spawn)
    routes.rs              8.698 satir   Tum route handler'lar
    webchat.rs               ~100 satir  Dashboard HTML montaji
  static/
    index_body.html          508 satir   Dashboard HTML (Alpine.js SPA)
    js/
      pages/sales.js         612 satir   Satis sayfasi JS mantigi
      api.js                 321 satir   OpenFangAPI istemci
      app.js                  70 satir   Ust seviye uygulama durumu
    css/
      theme.css                          Tema degiskenleri (acik/karanlik)
      layout.css                         Grid/flexbox duzenleri
      components.css                     Buton, input, tablo stilleri
    vendor/
      alpine.min.js                      Alpine.js framework
      marked.min.js                      Markdown ayristirici
      highlight.min.js                   Sozdizimi vurgulama
```

**Veritabani:** `~/.openfang/sales.db` (SQLite)
**LLM Modeli:** `gpt-5.3-codex` (OpenAI Codex OAuth uzerinden)
**LLM Saglayici:** `openai-codex`

## 1.2 Tum Sabitler (Gercek Kod, sales.rs satirlari 38-82)

```rust
const DEFAULT_LIMIT: usize = 100;                              // satir 38
const MIN_DOMAIN_RELEVANCE_SCORE: i32 = 5;                     // satir 39
const MAX_DISCOVERY_QUERIES: usize = 10;                       // satir 40
const MAX_ADAPTIVE_DISCOVERY_QUERIES: usize = 6;               // satir 41
const MAX_DISCOVERY_FAILURES_BEFORE_FAST_FALLBACK: u32 = 10;   // satir 42
const NO_BRAVE_FAIL_FAST_THRESHOLD: u32 = 1;                   // satir 43
const MAX_DIRECT_ENRICH_ATTEMPTS: usize = 12;                  // satir 44
const MAX_GENERIC_DIRECT_ENRICH_RETRIES: usize = 6;            // satir 45
const MAX_WEB_CONTACT_SEARCH_ATTEMPTS: usize = 12;             // satir 46
const DIRECT_ENRICH_TIMEOUT_MS: u64 = 3500;                    // satir 47
const MAX_EXTRA_SITE_ENRICH_PAGES: usize = 3;                  // satir 48
const MAX_PREFETCH_RETRY_CANDIDATES: usize = 3;                // satir 49
const MAX_PREFETCH_SITE_CANDIDATES: usize = 6;                 // satir 50
const SITE_PAGE_FETCH_TIMEOUT_MS: u64 = 1600;                  // satir 51
const FREE_DIRECTORY_FETCH_TIMEOUT_MS: u64 = 3200;             // satir 52
const MAX_FREE_DIRECTORY_CANDIDATES: usize = 30;               // satir 53
const MAX_TMB_DIRECTORY_CANDIDATES: usize = 8;                 // satir 54
const MAX_EUD_DIRECTORY_CANDIDATES: usize = 6;                 // satir 55
const MAX_ASMUD_DIRECTORY_CANDIDATES: usize = 10;              // satir 56
const MAX_PLATFORMDER_DIRECTORY_CANDIDATES: usize = 10;        // satir 57
const MAX_MIB_DIRECTORY_CANDIDATES: usize = 10;                // satir 58
const MAX_IMDER_DIRECTORY_CANDIDATES: usize = 8;               // satir 59
const MAX_ISDER_DIRECTORY_CANDIDATES: usize = 8;               // satir 60
const MAX_THBB_DIRECTORY_CANDIDATES: usize = 8;                // satir 61
const MAX_IMDER_DETAIL_FETCHES: usize = 12;                    // satir 62
const MAX_ISDER_DETAIL_FETCHES: usize = 12;                    // satir 63
const MIB_DIRECTORY_PAGE_COUNT: usize = 9;                     // satir 64
const MIB_DIRECTORY_PAGES_PER_RUN: usize = 3;                  // satir 65
const LEAD_QUERY_PLAN_TIMEOUT_SECS: u64 = 4;                   // satir 66
const LLM_COMPANY_GENERATION_TIMEOUT_SECS: u64 = 10;           // satir 67
const LLM_RELEVANCE_VALIDATION_TIMEOUT_SECS: u64 = 6;          // satir 68
const LLM_RELEVANCE_VALIDATION_BATCH_SIZE: usize = 40;         // satir 69
const MAX_LLM_PRIMARY_CANDIDATES: usize = 12;                  // satir 70
const SALES_DISCOVERY_SEARCH_TIMEOUT_SECS: u64 = 8;            // satir 71
const SALES_CONTACT_SEARCH_TIMEOUT_SECS: u64 = 4;              // satir 72
const SALES_OSINT_SEARCH_TIMEOUT_SECS: u64 = 5;                // satir 73
const SALES_SEARCH_BATCH_CONCURRENCY: usize = 3;               // satir 74
const SALES_OSINT_PROFILE_CONCURRENCY: usize = 4;              // satir 75
const PROSPECT_LLM_ENRICH_TIMEOUT_SECS: u64 = 18;              // satir 76
const MAX_OSINT_LINKS_PER_PROSPECT: usize = 6;                 // satir 77
const MAX_OSINT_SEARCH_TARGETS: usize = 24;                    // satir 78
const SALES_RUN_REQUEST_TIMEOUT_SECS: u64 = 240;               // satir 79
const SALES_RUN_RECOVERY_STALE_SECS: i64 = 255;                // satir 80
const SALES_LLM_PROVIDER: &str = "openai-codex";               // satir 81
const SALES_LLM_MODEL: &str = "gpt-5.3-codex";                 // satir 82
```

## 1.3 Tum Veri Yapilari (Gercek Rust Kodu)

### SalesProfile (satir 85-103)
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesProfile {
    pub product_name: String,                                    // Urun adi
    pub product_description: String,                             // Urun aciklamasi
    pub target_industry: String,                                 // Hedef sektor
    pub target_geo: String,                                      // Hedef cografya (varsayilan: "US")
    pub sender_name: String,                                     // Gonderen adi
    pub sender_email: String,                                    // Gonderen e-posta
    pub sender_linkedin: Option<String>,                         // LinkedIn (opsiyonel)
    #[serde(default = "default_target_title_policy")]
    pub target_title_policy: String,                             // "ceo_then_founder" veya "ceo_only"
    #[serde(default = "default_daily_target")]
    pub daily_target: u32,                                       // Gunluk hedef (varsayilan: 20)
    #[serde(default = "default_daily_send_cap")]
    pub daily_send_cap: u32,                                     // Gunluk gonderim limiti (varsayilan: 20)
    #[serde(default = "default_schedule_hour")]
    pub schedule_hour_local: u8,                                 // Zamanlama saati (varsayilan: 9)
    #[serde(default = "default_timezone_mode")]
    pub timezone_mode: String,                                   // "local" (varsayilan)
}
// Default impl: target_geo = "US" (satir 131)
// NOT: JS tarafinda varsayilan target_geo = "TR" (sales.js satir 17) — UYUMSUZLUK
```

### SalesRunRecord (satir 144-154)
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesRunRecord {
    pub id: String,                   // UUID
    pub status: String,               // "running" | "completed" | "failed"
    pub started_at: String,           // RFC3339 zaman damgasi
    pub completed_at: Option<String>, // Tamamlanma zamani
    pub discovered: u32,              // Bulunan aday sayisi
    pub inserted: u32,                // Olusturulan lead sayisi
    pub approvals_queued: u32,        // Kuyruga eklenen onay sayisi
    pub error: Option<String>,        // Hata mesaji veya calisma notu
}
```

### SalesLead (satir 156-175)
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesLead {
    pub id: String,                   // UUID
    pub run_id: String,               // Hangi calismadan
    pub company: String,              // Sirket adi
    pub website: String,              // "https://{domain}"
    pub company_domain: String,       // Alan adi
    pub contact_name: String,         // Kisi adi veya "Leadership Team"
    pub contact_title: String,        // Unvan (orn: "CEO/Founder")
    pub linkedin_url: Option<String>, // LinkedIn profil URL
    pub email: Option<String>,        // E-posta adresi
    pub phone: Option<String>,        // BUG: Her zaman None (satir 2315)
    pub reasons: Vec<String>,         // ICP uyum nedenleri (3-4 madde)
    pub email_subject: String,        // E-posta konu satiri
    pub email_body: String,           // E-posta govdesi
    pub linkedin_message: String,     // LinkedIn mesaji (maks 300 kar.)
    pub score: i32,                   // Puan (0-100, cap'li)
    pub status: String,               // "draft_ready"
    pub created_at: String,           // RFC3339
}
```

### SalesProspectProfile (satir 177-206)
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesProspectProfile {
    pub id: String,                              // company_domain (birincil anahtar)
    pub run_id: String,
    pub company: String,
    pub website: String,
    pub company_domain: String,
    pub fit_score: i32,                          // 0-100
    pub profile_status: String,                  // "contact_ready"|"contact_identified"|"company_only"
    pub summary: String,                         // Maks 280 karakter ozet
    pub matched_signals: Vec<String>,            // ICP esleme sinyalleri
    pub primary_contact_name: Option<String>,    // Kisi adi
    pub primary_contact_title: Option<String>,   // Unvan
    pub primary_email: Option<String>,           // E-posta
    pub primary_linkedin_url: Option<String>,    // LinkedIn profil
    pub company_linkedin_url: Option<String>,    // Sirket LinkedIn
    #[serde(default)]
    pub osint_links: Vec<String>,                // Kanit baglantilari (maks 6)
    pub contact_count: u32,                      // Bulunan kisi sayisi
    pub source_count: u32,                       // Sinyal kaynak sayisi
    pub buyer_roles: Vec<String>,                // Alici rolleri
    pub pain_points: Vec<String>,                // Aci noktalari
    pub trigger_events: Vec<String>,             // Tetikleyiciler
    pub recommended_channel: String,             // "email"|"linkedin"|"either"|"research"
    pub outreach_angle: String,                  // Erisim acisi
    pub research_status: String,                 // "heuristic"|"llm_enriched"
    pub research_confidence: f32,                // 0.0-1.0
    pub created_at: String,
    pub updated_at: String,
}
```

### SalesApproval (satir 208-217)
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesApproval {
    pub id: String,                   // UUID
    pub lead_id: String,              // Bagli lead
    pub channel: String,              // "email" veya "linkedin"
    pub payload: serde_json::Value,   // Kanal-ozel yuk (to/subject/body veya profile_url/message)
    pub status: String,               // "pending"|"approved"|"rejected"
    pub created_at: String,
    pub decided_at: Option<String>,   // Karar zamani
}
```

### SalesDelivery (satir 219-228)
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesDelivery {
    pub id: String,                   // UUID
    pub approval_id: String,          // Bagli onay
    pub channel: String,              // "email"|"linkedin"
    pub recipient: String,            // E-posta adresi veya LinkedIn URL
    pub status: String,               // "sent"|"failed"
    pub error: Option<String>,        // Hata mesaji
    pub sent_at: String,              // RFC3339
}
```

### DomainCandidate (satir 237-244) — Ic kullanim
```rust
#[derive(Debug, Clone, Default)]
struct DomainCandidate {
    domain: String,                   // Sirket alan adi
    score: i32,                       // Ilgilik puani
    evidence: Vec<String>,            // Kanit cumleleri
    matched_keywords: Vec<String>,    // Eslesen anahtar kelimeler
    source_links: Vec<String>,        // Kaynak URL'leri
}
```

### SalesEngine (satir 328-330) — Motor struct'i
```rust
pub struct SalesEngine {
    db_path: PathBuf,                 // ~/.openfang/sales.db
}

impl SalesEngine {
    pub fn new(home_dir: &FsPath) -> Self {
        Self { db_path: home_dir.join("sales.db") }
    }
}
```

## 1.4 Veritabani Semasi (Gercek SQL, satir 344-435)

```sql
-- 1. Tekil profil (id=1 kisitlamasi)
CREATE TABLE IF NOT EXISTS sales_profile (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    json TEXT NOT NULL,                    -- SalesProfile JSON
    updated_at TEXT NOT NULL               -- RFC3339
);

-- 2. Calisma gecmisi
CREATE TABLE IF NOT EXISTS sales_runs (
    id TEXT PRIMARY KEY,                   -- UUID
    status TEXT NOT NULL,                  -- running|completed|failed
    started_at TEXT NOT NULL,
    completed_at TEXT,
    discovered INTEGER NOT NULL DEFAULT 0,
    inserted INTEGER NOT NULL DEFAULT 0,
    approvals_queued INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

-- 3. Leadler (UNIQUE kisitlama var)
CREATE TABLE IF NOT EXISTS leads (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    company TEXT NOT NULL,
    website TEXT NOT NULL,
    company_domain TEXT NOT NULL,
    contact_name TEXT NOT NULL,
    contact_title TEXT NOT NULL,
    linkedin_url TEXT,
    email TEXT,
    phone TEXT,
    reasons_json TEXT NOT NULL,            -- JSON dizisi
    email_subject TEXT NOT NULL,
    email_body TEXT NOT NULL,
    linkedin_message TEXT NOT NULL,
    score INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(company_domain, contact_name, contact_title)  -- Tekillik
);

-- 4. Onay kuyrugu (UNIQUE kisitlama YOK — BUG-06)
CREATE TABLE IF NOT EXISTS approvals (
    id TEXT PRIMARY KEY,
    lead_id TEXT NOT NULL,
    channel TEXT NOT NULL,                 -- email|linkedin
    payload_json TEXT NOT NULL,            -- JSON (to/subject/body veya profile_url/message)
    status TEXT NOT NULL,                  -- pending|approved|rejected
    created_at TEXT NOT NULL,
    decided_at TEXT
);

-- 5. Teslimat kaydi
CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    approval_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    recipient TEXT NOT NULL,
    status TEXT NOT NULL,                  -- sent|failed
    error TEXT,
    sent_at TEXT NOT NULL
);

-- 6. Onboarding brief (tekil)
CREATE TABLE IF NOT EXISTS sales_onboarding (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    brief_text TEXT,
    updated_at TEXT NOT NULL
);

-- 7. Domain tekilsizlestirme
CREATE TABLE IF NOT EXISTS discovered_domains (
    domain TEXT PRIMARY KEY,
    first_seen_run_id TEXT NOT NULL,
    first_seen_at TEXT NOT NULL
);

-- 8. Prospect profilleri
CREATE TABLE IF NOT EXISTS prospect_profiles (
    company_domain TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    json TEXT NOT NULL,                    -- SalesProspectProfile JSON
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Indexler
CREATE INDEX IF NOT EXISTS idx_approvals_status_created
    ON approvals(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_leads_created
    ON leads(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_deliveries_sent
    ON deliveries(sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_prospect_profiles_run_updated
    ON prospect_profiles(run_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_prospect_profiles_updated
    ON prospect_profiles(updated_at DESC);
```

## 1.5 API Endpoint Listesi (satir numaralari ile)

| Handler | Satir | Yontem | Yol | Amac |
|---------|-------|--------|-----|------|
| `get_sales_profile` | 11702 | GET | `/api/sales/profile` | Profili getir |
| `put_sales_profile` | 11725 | PUT | `/api/sales/profile` | Profili kaydet |
| `autofill_sales_profile` | 11524 | POST | `/api/sales/profile/autofill` | Brieften doldur |
| `get_sales_onboarding_status` | 11652 | GET | `/api/sales/onboarding/status` | Onboarding durumu |
| `put_sales_onboarding_brief` | 11593 | POST | `/api/sales/onboarding/brief` | Brief kaydet |
| `run_sales_now` | 11761 | POST | `/api/sales/run` | Calisma baslat |
| `list_sales_runs` | 11813 | GET | `/api/sales/runs?limit=N` | Gecmis |
| `list_sales_leads` | 11841 | GET | `/api/sales/leads?limit=N&run_id=X` | Leadler |
| `list_sales_prospects` | 11869 | GET | `/api/sales/prospects?limit=N&run_id=X` | Profiller |
| `list_sales_approvals` | 11897 | GET | `/api/sales/approvals?limit=N` | Onaylar |
| `approve_and_send` | 11924 | POST | `/api/sales/approvals/{id}/approve` | Onayla+gonder |
| `reject_sales_approval` | 11947 | POST | `/api/sales/approvals/{id}/reject` | Reddet |
| `list_sales_deliveries` | 11974 | GET | `/api/sales/deliveries?limit=N` | Teslimatlar |
| `spawn_sales_scheduler` | 12001 | — | (arka plan gorevi) | Gunluk otomasyon |

**OAuth Endpointleri (codex_oauth.rs):**
| Handler | Yontem | Yol |
|---------|--------|-----|
| `codex_oauth_start` | POST | `/api/auth/codex/start` |
| `codex_oauth_callback` | GET | `/api/auth/codex/callback` + `/auth/callback` |
| `codex_oauth_paste_code` | POST | `/api/auth/codex/paste-code` |
| `codex_oauth_import_cli` | POST | `/api/auth/codex/import-cli` |
| `codex_oauth_status` | GET | `/api/auth/codex/status` |
| `codex_oauth_logout` | POST | `/api/auth/codex/logout` |

---

# BOLUM II — PIPELINE MEKANIZMASI

## 2.1 Pipeline Genel Akis

`run_generation()` fonksiyonu (satir 1470-2402), tek bir calismanin tam akisini icerir:

```
GIRDI: SalesProfile (kullanici yapilandirmasi)
CIKTI: SalesRunRecord (sonuc ozeti)

ADIM 0: Dogrulama
  - product_name, product_description, target_industry bos mu?
  - Bossa hata dondur

ADIM 1: Sorgu Plani [4sn timeout]
  - LLM ile discovery_queries olustur
  - Basarisizsa sezgisel yedek

ADIM 2: Paralel Kesif [tokio::join!]
  2a. LLM sirket uretimi [10sn, maks 12]
  2b. Web arama [8sn, 10 sorgu]
  2c. Turk dizin taramasi [3.2sn, 8 kaynak]

ADIM 3: Birlestirme
  - Domain bazli tekilsizlestirme
  - Kaynak iletisim ipuclarini birlestir

ADIM 4: LLM Ilgilik Dogrulamasi [6sn, maks 40 aday]
  - relevant + confidence >= 0.5 → dogrulanmis

ADIM 5: Filtreleme + Siralama
  - score >= kalite esigi
  - Engellenen domainler cikarilir
  - Siralama: ontohumlama onceligi > puan > domain

ADIM 6: Prospect Profil Tohumlama [18sn LLM]
  - Sezgisel profil olustur
  - LLM ile zenginlestir (ozet, pain point, trigger, angle)
  - SQLite'a kaydet

ADIM 7: Lead Uretim Dongusu [her aday icin]
  - Web iletisim arama (3 sorgu seti)
  - Site HTML zenginlestirme (ana+ekip+hakkinda)
  - Normalizasyon ve dogrulama
  - Company signal kontrolu
  - Outreach kanal kontrolu
  - Lead olusturma (UNIQUE kisitlama)
  - Onay kuyruguna ekleme (email + linkedin)
```

## 2.2 Asama 1: Sorgu Planlama

### LLM Prompt (satir 10767-10792)

**Sistem istemi:**
```
"You are an elite outbound prospecting strategist and business development
operator. Output strict valid JSON only."
```

**Kullanici istemi:**
```
"You are generating a B2B outbound lead discovery plan.
Product: {product_name}
Product value proposition: {product_description}
Target industry: {target_industry}
Target geography: {target_geo}
Target title policy: {target_title_policy}

Return strict JSON only with keys:
discovery_queries (array of 6-10 web queries to find PROSPECT COMPANIES,
  not blogs/directories),
must_include_keywords (array),
exclude_keywords (array),
contact_titles (array).

Rules:
- Think like an elite business development rep hunting reachable ICP-fit
  accounts, not a generic researcher.
- Cover multiple plausible subsegments, company archetypes, and buying triggers.
- If product suggests field/on-site operations, prioritize companies with
  field teams.
- Prefer company-finding queries that mention operational pain, company type,
  or sub-industry rather than generic head terms.
- discovery_queries should include both English and local-language variants
  when helpful.
- exclude_keywords should remove directories/news/job pages/review sites.
- Output valid JSON only."
```

**LLM Yapilandirmasi:** Model: gpt-5.3-codex, max_tokens: 900, temperature: 0.0, reasoning: Medium

**Cikti Yapisi:**
```json
{
  "discovery_queries": ["query1", "query2", ...],
  "must_include_keywords": ["keyword1", ...],
  "exclude_keywords": ["wikipedia", "linkedin", ...],
  "contact_titles": ["CEO", "Founder", ...]
}
```

**Sezgisel Yedek:** `heuristic_lead_query_plan()` — LLM basarisiz veya 4sn asilirsa

**Ozel Durum:** `is_field_ops && geo_is_turkey` ise LLM sorgu plani ATLANIR, dogrudan sezgisel plan kullanilir

## 2.3 Asama 2: Paralel Kesif

Uc kanal `tokio::join!` ile esanli calisir:

### Kanal A: LLM Sirket Uretimi (satir 10863-10967)

**Sistem istemi:**
```
"You are an elite B2B market mapper and business development operator.
Suggest realistic ICP-fit prospect companies with accurate domains.
Output strict valid JSON only."
```

**Kullanici istemi:**
```
"List up to {max_co} real B2B companies for outbound sales prospecting.
Product: {product_name}
Product value: {product_description}
Target industry: {target_industry}
Target geography: {target_geo}
Run #{run_sequence}.
{previously_discovered_domains}

Return strict JSON: {"companies":[{"company":"...","domain":"...","reason":"..."}]}

CRITICAL RULES:
- Think like a top-tier business development rep building a pipeline
  for immediate outreach.
- Focus on real SMB/mid-market companies (20-5000 employees) that
  operate in or sell into {geo}
- NO global giants (Siemens, ABB, Honeywell, Schneider, Bosch, etc.)
- domain must be real company website (.com.tr or .tr or .com).
  NO linkedin/wikipedia/news
- reason: 1 short clause explaining their likely operational pain
  or why they are a fit now
- Spread suggestions across multiple cities, sub-industries, and
  company archetypes when possible
- Prefer companies that look reachable for outbound today"
```

**LLM Yapilandirmasi:** max_tokens: 2400, temperature: 0.2, reasoning: Medium

**Uretilen aday puani:** `MIN_DOMAIN_RELEVANCE_SCORE + 12 = 17`

**max_co hesaplama:** `(daily_target + 4).clamp(6, 12)`

### Kanal B: Web Arama Kesfii

**Fonksiyon:** `discover_via_web_search()`
- Arama motoru: Yapilandirilmis saglayici + Brave yedek
- Sorgu sayisi: Plandan gelen discovery_queries (maks 10)
- Timeout: `SALES_DISCOVERY_SEARCH_TIMEOUT_SECS = 8sn`
- Donus: (adaylar, iletisim ipuclari, arama_kullanilamaz bayragi)

### Kanal C: Turk Dizin Taramasi

**Fonksiyon:** `fetch_free_discovery_candidates()` (satir 4230)
- Sadece `geo_is_turkey()` ise calisir
- 8 kaynak paralel olarak taranir (detaylar Bolum III'te)
- Toplam maks: `MAX_FREE_DIRECTORY_CANDIDATES = 30`

## 2.4 Asama 3: Birlestirme

**Fonksiyon:** `merge_all_discovery_sources()`
- LLM + Web + Dizin adaylari birlestir
- Domain bazli tekilsizlestirme
- Ayni domainden gelen evidence ve matched_keywords birlestir
- Kaynak iletisim ipuclarini (isim, unvan, e-posta) aktar

## 2.5 Asama 4: LLM Ilgilik Dogrulamasi (satir 10987-11057)

**Sistem istemi:**
```
"You are a Turkish B2B market analyst. Rate company relevance for the
given ICP. Output strict valid JSON only."
```

**Kullanici istemi:**
```
"Rate each company for ICP fit as a B2B sales prospect.

Our product: {product_name} - {product_description}
Target industry: {target_industry}
Target geography: {target_geo}
We sell to companies with field/on-site operations (construction,
maintenance, facility management, etc.)

Companies to evaluate:
{candidates_json}

For each company, assess:
- Is it a real company in our target industry with field operations teams?
- Would they benefit from our product?

Return strict JSON only:
{"results":[{"domain":"...","relevant":true/false,"confidence":0.0-1.0,
"reason":"..."}]}"
```

**Puanlama etkisi:**
- relevant + confidence >= 0.7: **+15 puan**
- relevant + confidence >= 0.4: **+8 puan**
- not relevant + confidence >= 0.7: **-15 puan**
- not relevant + confidence < 0.7: **-5 puan**

**Atlama kosulu:** `is_field_ops && geo_is_turkey` ise bu adim ATLANIR

## 2.6 Asama 5: Filtreleme ve Siralama

```
1. Kalite esigi: candidate.score >= candidate_quality_floor(&profile)
2. candidate_should_skip_for_profile() kontrolu
3. Siralama:
   - Birincil: candidate_preseed_priority() (azalan)
   - Ikincil: score (azalan)
   - Ucuncul: domain (artan, alfabetik)
4. Bossa: calisma "failed" olarak sonlanir
```

## 2.7 Asama 6: Prospect Profil Tohumlama

**Fonksiyon:** `seed_prospect_profiles_for_run()` (satir 2404+)

1. Her aday icin sezgisel profil olustur
2. Iletisim ipuclarini uygula
3. LLM zenginlestirme: `llm_enrich_prospect_profiles()` (maks 6 profil, 18sn timeout)
4. SQLite'a kaydet

### LLM Zenginlestirme Promptu (satir 11151-11175)

**Sistem istemi:**
```
"You are a B2B prospect research analyst. Build concise, evidence-bound
account dossiers from partial outbound signals.
Output strict valid JSON only."
```

**Kullanici istemi:**
```
"Create outbound account dossiers for candidate customers.
Product: {product_name}
Product value proposition: {product_description}
Target industry: {target_industry}
Target geography: {target_geo}

Candidate accounts:
{candidates_json}

Return strict JSON only:
{"profiles":[{
  "company_domain":"...",
  "summary":"...",
  "buyer_roles":["..."],
  "pain_points":["..."],
  "trigger_events":["..."],
  "recommended_channel":"email|linkedin|either|research",
  "outreach_angle":"...",
  "research_confidence":0.0
}]}

Rules:
- Use ONLY the provided signals and contact context.
  Do not invent news, numbers, customers, headcount, funding,
  or software stack.
- summary: 1 short paragraph, max 220 chars
- buyer_roles: 2-4 roles likely to care
- pain_points: 2-3 pains tied to the product and public signals
- trigger_events: 2-3 short lines explaining why now
- outreach_angle: 1 concise angle for first-touch personalization
- recommended_channel: email|linkedin|either|research
- research_confidence: 0.0-1.0
- Output JSON only."
```

## 2.8 Asama 7: Lead Uretim Dongusu

Her aday icin (satir 1748-2341):

```
for candidate in candidate_list.take(max_candidates):
    if inserted >= daily_target: break

    // 1. Tohumlanan profil verilerini al
    seeded = prospect_profile_lookup.get(domain)

    // 2. Iletisim zenginlestirme (skip_web_contact_search degilse):
    //    - 3 sorgu seti olustur (LinkedIn, site, genel)
    //    - run_sales_search_batch() ile toplu arama
    //    - Sonuclardan isim/unvan/linkedin/email cikar
    //    - Yedek arama (fallback_contact_query)
    //    - Sirket LinkedIn arama

    // 3. Kaynak ipuclarini uygula
    // 4. Ongetirme zenginlestirmesini uygula
    // 5. Dogrudan site zenginlestirme (HTML indirme)
    // 6. Genel kimlik yeniden deneme
    // 7. Arama zenginlestirme sinyallerini uygula

    // 8. Normalizasyon:
    //    - contact_name → normalize_person_name()
    //    - linkedin_url → normalize_outreach_linkedin_url()
    //    - email → normalize_contact_email_for_domain() veya normalize_site_contact_email()

    // 9. Prospect profili guncelle (varsa)

    // 10. Dogrulama kontrolleri:
    //     - Sirket dogrulama sinyali VAR MI? (yoksa atla)
    //     - Erisim kanali VAR MI? (email veya linkedin, yoksa atla)
    //     - Kisi kimligi VAR MI? (isim veya linkedin, yoksa atla)

    // 11. Lead olustur:
    score = (lead_score(&linkedin, &email) + candidate.score).min(100)
    contact_name = contact_name.unwrap_or("Leadership Team")
    contact_title = contact_title.unwrap_or(default_title)
    phone = None  // BUG: hardcoded

    // 12. Lead ekle (UNIQUE kisitlama):
    //     Ok(true) → inserted++, record_discovered_domain, queue_approvals
    //     Ok(false) → duplicate, sessizce atla
```

---

# BOLUM III — TURK DIZIN TARAYICILARI

## 3.1 Orchestrator

**Fonksiyon:** `fetch_free_discovery_candidates()` (satir 4230-4319)
- Sadece `geo_is_turkey(&profile.target_geo)` ise calisir
- HTTP istemcisi: Mozilla user-agent, `FREE_DIRECTORY_FETCH_TIMEOUT_MS = 3200ms`
- 8 kaynak `tokio::join!` ile paralel cagirilir
- Her kaynak `profile_targets_field_ops()` veya `profile_targets_energy()` kosuluna bagli
- Sonuclar `interleave_free_discovery_sources()` ile birlestirilir
- Toplam maks: 30 aday

## 3.2 TMB Tarayici

**URL:** `https://www.tmb.org.tr/en/members`
**Kosul:** `profile_targets_field_ops()`
**Maks:** 8 aday
**Puan:** `MIN_DOMAIN_RELEVANCE_SCORE + 14 = 19`
**Regex kaliplari:**
- `<article class="member-card"...>(.*?)</article>` — Uye karti
- `<div class="name">...<a href="([^"]+)">` — Detay URL
- `<div class="name">...<a>(.*?)</a>` — Sirket adi
- `Chairman of the Board\s*:\s*<strong>(.*?)</strong>` — Baskan adi
- `<th>Web</th>...<a href="([^"]+)"` — Sirket web sitesi
**Cikarilan veriler:** Domain, sirket adi, baskan adi (Chairman), e-posta
**Evidence ornegi:** "TMB members directory lists {sirket} as a Turkish contractor member with website {domain}"
**Keywords:** "construction", "infrastructure", "contractor association"

## 3.3 EUD Tarayici

**URL:** `https://www.eud.org.tr/en/members`
**Kosul:** `profile_targets_energy()`
**Maks:** 6 aday
**Puan:** `MIN_DOMAIN_RELEVANCE_SCORE + 13 = 18`
**Regex:** `<a href="([^']+)" ...><div class="member-box"` — Uye linki
**Cikarilan veriler:** Sadece domain (isim/email/telefon yok)
**Keywords:** "energy", "utility", "power generation"

## 3.4 ASMUD Tarayici

**URL:** `https://www.asmud.org.tr/Uyeler.asp`
**Kosul:** `profile_targets_field_ops()`
**Maks:** 10 aday
**Puan:** `MIN_DOMAIN_RELEVANCE_SCORE + 14 = 19`
**Regex kaliplari:**
- `<div class="uwrap w3-card">` — Uye bolumu
- `<a href="([^"]+)">` — Web sitesi
- `<strong>(.*?)</strong>` — Sirket adi
- `(?:^|<br>\s*)T:\s*([^<]+)` — Telefon
- `(?:^|<br>\s*)E:\s*<span>(.*?)</span>` — E-posta
**Cikarilan veriler:** Domain, sirket adi, telefon, e-posta
**Keywords:** "asphalt", "road construction", "infrastructure" + kosula bagli ekler

## 3.5 Platformder Tarayici

**URL:** `https://www.platformder.org.tr/rehber/`
**Kosul:** `profile_targets_field_ops()`
**Maks:** 10 aday
**Puan:** `MIN_DOMAIN_RELEVANCE_SCORE + 12 = 17`
**Regex:** `<li id="item-\d+-\d+" data-title="([^"]+)" data-phone="([^"]*)" data-url="([^"]*)">(.*?)</li>`
**Cikarilan veriler:** Sirket adi (data-title), telefon (data-phone), web sitesi (data-url)
**Keywords:** Kosula bagli: "access platform", "crane", "forklift", "lift", "equipment"
**NOT:** gmail.com burada sirket domaini olarak gelebilir — **BUG-01**

## 3.6 MIB Tarayici (Sayfa Rotasyonu)

**URL:** `https://mib.org.tr/en/our-members/` (sayfa 1) + `/{page}/` (sayfa 2-9)
**Kosul:** `profile_targets_field_ops()`
**Maks:** 10 aday
**Puan:** `MIN_DOMAIN_RELEVANCE_SCORE + 13 = 18`
**Sayfa Rotasyonu:** `MIB_DIRECTORY_PAGES_PER_RUN=3` sayfa/calisma, `run_sequence` bazli rotasyon
**Regex:**
- `<h2><a href="([^"]+)">(.*?)</a></h2>` — Sirket linki + adi
- `<a href="([^"]+)"><span><i class="fa-globe"` — Web sitesi
- `href="mailto:([^"]+)"` — E-posta
**Cikarilan veriler:** Domain, sirket adi, e-posta
**Keywords:** "industrial equipment", "field equipment", "machinery association" + kosula bagli

## 3.7 IMDER Tarayici (Detay Sayfalari)

**Birincil URL:** `https://imder.org.tr/uyelerimiz/`
**Kosul:** `profile_targets_field_ops()`
**Maks:** 8 aday, `MAX_IMDER_DETAIL_FETCHES=12` detay sayfasi
**Puan:** `MIN_DOMAIN_RELEVANCE_SCORE + 14 = 19`
**Iki Asamali:**
1. Index sayfasindan detay URL'lerini cikar
2. Her detay sayfasini ayri indir ve ayristir
**Detay sayfasi regex:**
- `<h1 class="elementor-heading-title">(.*?)</h1>` — Sirket adi
- `<strong>Isim Soyisim</strong>...</td><td>(.*?)</td>` — Kisi adi
- `<strong>G(o|ö)revi</strong>...</td><td>(.*?)</td>` — Gorevi
- `<strong>Telefon</strong>...</td><td>(.*?)</td>` — Telefon
- `<strong>Web Sitesi</strong>...</td><td><a href="([^"]+)"` — Web sitesi
**Cikarilan veriler:** Domain, sirket adi, kisi adi, unvan, telefon, e-posta

## 3.8 ISDER Tarayici

**Birincil URL:** `https://isder.org.tr/uyelerimiz/`
**Yapi:** IMDER ile neredeyse ayni (detay sayfali)
**Maks:** 8 aday, `MAX_ISDER_DETAIL_FETCHES=12`
**Puan:** `MIN_DOMAIN_RELEVANCE_SCORE + 13 = 18`
**Regex farki:** Alan etiketlerinde opsiyonel iki nokta ("Isim Soyisim:" vs "Isim Soyisim")
**Keywords:** "industrial equipment", "material handling" + kosula bagli

## 3.9 THBB Tarayici

**URL:** `https://www.thbb.org/uyelerimiz/yazismali-uyeler/`
**Kosul:** `profile_targets_field_ops()`
**Puan:** `MIN_DOMAIN_RELEVANCE_SCORE + 13 = 18`
**Regex:**
- `<strong>Web:?\s*</strong>...<a href="([^"]+)"` — Web sitesi
- `<strong>Tel:?\s*</strong>...([^<]+)` — Telefon
- `<strong>(.*?)</strong>` — Sirket adi (alan etiketleri filtrelenerek)
**Cikarilan veriler:** Domain, sirket adi, telefon, e-posta
**Keywords:** "construction equipment", "concrete equipment" + kosula bagli

### Dizin Ozet Tablosu

| Dizin | URL | Kisi | Tel | Email | Detay Sayfa | Maks | Puan |
|-------|-----|------|-----|-------|-------------|------|------|
| TMB | tmb.org.tr/en/members | Evet (Baskan) | Hayir | Evet | Hayir | 8 | 19 |
| EUD | eud.org.tr/en/members | Hayir | Hayir | Hayir | Hayir | 6 | 18 |
| ASMUD | asmud.org.tr/Uyeler.asp | Hayir | Evet | Evet | Hayir | 10 | 19 |
| Platformder | platformder.org.tr/rehber | Hayir | Evet | Hayir | Hayir | 10 | 17 |
| MIB | mib.org.tr/en/our-members | Hayir | Hayir | Evet | Hayir | 10 | 18 |
| IMDER | imder.org.tr/uyelerimiz | Evet | Evet | Evet | Evet (12) | 8 | 19 |
| ISDER | isder.org.tr/uyelerimiz | Evet | Evet | Evet | Evet (12) | 8 | 18 |
| THBB | thbb.org/uyelerimiz/yazismali | Hayir | Evet | Evet | Hayir | 8 | 18 |

---

# BOLUM IV — FILTRELEME VE PUANLAMA

## 4.1 lead_score() (satir 8631-8640)

```rust
fn lead_score(linkedin: &Option<String>, email: &Option<String>) -> i32 {
    let mut s = 60;                    // Temel puan
    if linkedin.is_some() { s += 20; } // LinkedIn varsa +20
    if email.is_some() { s += 20; }    // Email varsa +20 (info@ dahil!)
    s                                   // Maks: 100
}
// Nihai: (lead_score + candidate.score).min(100)
// SORUN: info@ bile +20 aliyor, neredeyse herkes 100 oluyor
```

## 4.2 prospect_status() (satir 3210-3231)

```rust
fn prospect_status(name: Option<&str>, email: Option<&str>,
                   linkedin: Option<&str>) -> &'static str {
    if email.map(email_is_actionable_outreach_email).unwrap_or(false)
       || linkedin.and_then(normalize_outreach_linkedin_url).is_some()
    { "contact_ready" }
    else if name.map(|v| !contact_name_is_placeholder(Some(v))).unwrap_or(false)
    { "contact_identified" }
    else
    { "company_only" }
}
// SORUN: email_is_actionable_outreach_email() info@ engelliyor AMA
// dizin taramasindan gelen e-postalar bu kontrolden gecmiyor (BUG-02)
```

## 4.3 email_is_actionable_outreach_email() (satir 7123-7128)

```rust
fn email_is_actionable_outreach_email(email: &str) -> bool {
    let Some(domain) = email_domain(email) else { return false; };
    !is_consumer_email_domain(&domain)     // gmail.com vb. degil
    && !email_is_generic_role_mailbox(email) // info@ vb. degil
}
```

## 4.4 email_is_generic_role_mailbox() (satir 7097-7121)

```rust
fn email_is_generic_role_mailbox(email: &str) -> bool {
    let Some(local) = email_local_part(email) else { return false; };
    let normalized = local.split(['+', '.', '-', '_']).next()
        .unwrap_or(local.as_str()).trim();
    matches!(normalized,
        "info" | "hello" | "contact" | "office" | "mail"
        | "admin" | "support" | "sales" | "team" | "general"
        | "iletisim" | "merhaba"
    )
}
```

## 4.5 is_consumer_email_domain() (satir 3662-3692)

**Engellenen domainler (tam liste):**
```
gmail.com, googlemail.com, yahoo.com, ymail.com, rocketmail.com,
hotmail.com, outlook.com, live.com, msn.com, icloud.com, me.com,
mac.com, protonmail.com, proton.me, mail.com, aol.com, gmx.com,
gmx.net, yandex.com, yandex.ru, qq.com, 163.com
```

## 4.6 is_blocked_company_domain() (satir 3694-3784)

**Engellenen domainler (tam liste):**
```
Sosyal: linkedin.com, facebook.com, instagram.com, x.com, twitter.com,
        youtube.com, reddit.com, medium.com
Haber: forbes.com, bloomberg.com, wsj.com, techcrunch.com
Arastirma: crunchbase.com, mordorintelligence.com, techsciresearch.com,
           researchandmarkets.com, grandviewresearch.com, gminsights.com,
           marketsandmarkets.com, fortunebusinessinsights.com, statista.com,
           expertmarketresearch.com
Platform: g2.com, capterra.com, producthunt.com
Referans: wikipedia.org, definitions.net, merriam-webster.com,
          cambridge.org, dictionary.com, wiktionary.org
Is: indeed.com, glassdoor.com, angel.co, wellfound.com, ycombinator.com
Arama: duckduckgo.com, google.com, bing.com, yahoo.com

Global devler (domain icinde gecerse):
boschrexroth, bosch, siemens, abb, honeywell, schneider-electric,
schneider, cargill, mitsubishi, hitachi, philips, toyota-forklift,
toyota-industries

Ek kurallar: blog.* ile baslayan, "dictionary/definitions/wiktionary/
marketresearch" iceren domainler
```

## 4.7 contact_name_is_placeholder() (satir 4154-4190)

**Engellenen isimler (tam liste):**
```
Ingilizce: unknown, leadership, leadership team, management,
           management team, executive team, executive committee,
           board of directors
Turkce:    ust yonetim, üst yönetim, yonetim ekibi, yönetim ekibi,
           yonetim takimi, yönetim takımı, yonetim kurulu,
           yönetim kurulu, icra kurulu, i̇cra kurulu
```

**EKSIK (BUG-03):** "baskanin mesaji", "genel mudurun mesaji",
"hakkimizda", "vizyonumuz", "misyonumuz" gibi sayfa basliklari listede yok.

## 4.8 Cografya ve Sektor Algilama

```rust
// geo_is_turkey() — satir 9341
fn geo_is_turkey(geo: &str) -> bool {
    let n = geo.trim().to_lowercase();
    n.contains("tr") || n.contains("turkiye")
    || n.contains("türkiye") || n.contains("turkey")
}

// profile_targets_field_ops() — satir 9244
fn profile_targets_field_ops(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed.contains("saha") || seed.contains("field")
    || seed.contains("operasyon") || seed.contains("operations")
    || seed.contains("maintenance") || seed.contains("construction")
    || seed.contains("facility") || seed.contains("dispatch")
    || seed.contains("on-site")
}

// profile_targets_energy() — satir 5748
fn profile_targets_energy(profile: &SalesProfile) -> bool {
    let seed = profile_keyword_seed_text(profile);
    seed.contains("energy") || seed.contains("enerji")
    || seed.contains("electric") || seed.contains("electricity")
    || seed.contains("power") || seed.contains("utility")
    || seed.contains("renewable")
}
```

---

# BOLUM V — MESAJ URETIMI VE TESLIMAT

## 5.1 E-posta Konu Satiri (satir 5905-5911)

```rust
fn build_sales_email_subject(profile: &SalesProfile, company: &str) -> String {
    if geo_is_turkey(&profile.target_geo) {
        format!("{company} icin saha operasyon koordinasyonu")
    } else {
        format!("{company}: field ops coordination")
    }
}
```

## 5.2 E-posta Govdesi (satir 5913-5951)

**Turkce Sablon:**
```
Merhaba {alici},

{company} ile ilgili su sinyali gordum: {kanit_kisa_160}.

{company} tarafinda {eslesen_kisa_90} operasyonlarinda is atama, takip
ve gecikme yonetimi kolayca daginik hale gelebiliyor.
{urun_adi} burada su ise yarayabilir: {urun_aciklama_kisa_150}.

Uygunsa {company} icin 3 maddelik kisa bir operasyon akisi onerisi
paylasabilirim.

Selamlar,
{gonderen_adi}
```

**Ingilizce Sablon:**
```
Hi {recipient},

I came across {company} through this public signal: {evidence_short_160}.

For teams running {matched_short_90}, the friction is usually around
task ownership, follow-up, and delay recovery across email, phone,
and chat. {product_name} could help here: {value_short_150}.

If useful, I can send a short 3-point workflow teardown for {company}.

Best,
{sender_name}
```

**Alici adi secimi** (`outreach_recipient_name()`, satir 5862):
- Kisi adi varsa: ilk isim ("Ali Vural" → "Ali")
- Yoksa: "{sirket} ekibi" (TR) veya "{company} team" (EN)

## 5.3 LinkedIn Mesaji (satir 5953-5978)

**Turkce (maks 300 karakter):**
```
Merhaba {alici}, {company} ile ilgili su sinyali gordum: {kanit_kisa_110}.
{urun_adi} saha ekiplerinde takip ve koordinasyonu toparlamaya yardimci
oluyor. Uygunsa kisa bir akis onerisi paylasabilirim.
```

## 5.4 ICP Sinyal Uretimi (satir 5876-5903)

```rust
fn build_sales_lead_reasons(...) -> Vec<String> {
    vec![
        format!("ICP fit: {matched_kisa_140}"),
        format!("Public evidence: {kanit_kisa_220}"),
        // Unvan varsa ve genel degilse:
        format!("Potential buyer role: {title}"),
        format!("Value hypothesis: {urun} could help {sirket} with {aciklama_kisa_140}")
    ]
}
```

## 5.5 Pain Point Uretimi (satir 3367-3403)

**Sablon bazli (LLM degil):**
```rust
fn build_prospect_pain_points(...) -> Vec<String> {
    vec![
        "{company} tarafinda saha ekipleri, gorev akisi ve dispatch
         gorunurlugu parcali olabilir.",
        "{top_sinyal} ile iliskili tekrar eden takip isleri icin
         {urun_adi} degeri guclu gorunuyor.",
        "Mevcut surecler muhtemelen WhatsApp, e-posta ve manuel
         koordinasyon arasinda daginik; {urun_aciklama} burada
         dogrudan deger tasiyabilir."
    ]
}
// SORUN: Tum sirketler icin ayni 3 cumle, sadece sirket adi degisiyor
```

## 5.6 Trigger Event Uretimi (satir 3405-3435)

```rust
fn build_prospect_trigger_events(...) -> Vec<String> {
    vec![
        "Kamuya acik sinyal: {top_sinyal_kisa_100}",
        "{unvan} seviyesinde sahiplenme ihtimali var.",
        // Duruma gore:
        // contact_ready: "Dogrudan outbound baslatilabilecek kanal bulundu."
        // contact_identified: "Karar verici bulundu, kanal dogrulamasi kaldi."
        // company_only: "Sirket seviyesi sinyal var; buying committee
        //   haritaLamasi gerekiyor."
    ]
}
```

## 5.7 Outreach Angle Uretimi (satir 3437-3472)

```rust
fn build_prospect_outreach_angle(...) -> String {
    // "{company} icin ilk temas: '{top_pain}' ve '{top_trigger}'
    //  uzerinden {urun_adi} degerini {kanal} ile 1 kisa operasyon
    //  iyilestirme hipotezi paylas."
    // Maks 220 karakter
}
```

## 5.8 Prospect Ozet Uretimi (satir 3259-3298)

```rust
fn build_prospect_summary(...) -> String {
    // "{company} fits via {sinyaller_3}.
    //  Primary contact: {isim} ({unvan}).
    //  Channels: {email + linkedin}."
    // Maks 280 karakter
}
```

## 5.9 send_email() — SMTP Kodu (satir 1133-1175)

```rust
async fn send_email(&self, state: &AppState, to: &str,
                    subject: &str, body: &str) -> Result<(), String> {
    // 1. channels_config'den email ayarlarini al
    // 2. Sifre cevresel degiskenden (cfg.password_env)
    // 3. Gonderen ve alici Mailbox olarak ayristir
    // 4. Message::builder() ile e-posta olustur
    // 5. AsyncSmtpTransport::starttls_relay() ile SMTP baglantisi
    // 6. transport.send(msg).await
}
```

## 5.10 send_linkedin() — Tarayici Otomasyon Kodu (satir 1177-1235)

```rust
async fn send_linkedin(&self, state: &AppState, profile_url: &str,
                       message: &str) -> Result<(), String> {
    let agent_id = "sales_linkedin";
    // 1. BrowserCommand::Navigate { url: profile_url }
    // 2. BrowserCommand::Click { selector: "Message" }
    // 3. BrowserCommand::Type {
    //      selector: "div.msg-form__contenteditable[contenteditable='true']",
    //      text: message
    //    }
    // 4. BrowserCommand::Click { selector: "button.msg-form__send-button" }
}
```

## 5.11 Onay Akisi

### queue_approvals_for_lead() (satir 755-792)
```
Lead icin:
  email varsa → approvals tablosuna email kanali ile ekle
  linkedin varsa → approvals tablosuna linkedin kanali ile ekle
  Donus: eklenen onay sayisi (0, 1 veya 2)

Payload ornekleri:
  Email:    {"to": "ali@abc.com", "subject": "...", "body": "..."}
  LinkedIn: {"profile_url": "https://linkedin.com/in/ali", "message": "..."}
```

### approve_and_send() (satir 1272-1393)
```
1. Onay kaydini sorgula (id ile)
2. Durumun "pending" oldugunu dogrula
3. Gunluk gonderim cap'ini kontrol et (deliveries_today >= daily_send_cap?)
4. Kanala gore gonder:
   email → send_email(to, subject, body)
   linkedin → send_linkedin(profile_url, message)
5. Basarili: durum "approved", teslimat kaydi "sent"
6. Basarisiz: teslimat kaydi "failed" + hata mesaji
```

### reject_approval() (satir 1395-1412)
```
1. Onay kaydini sorgula
2. "pending" degilse hata dondur
3. Durumu "rejected" olarak guncelle
```

---

# BOLUM VI — LLM ENTEGRASYONU

## 6.1 LLM Yapilandirmasi

```
Saglayici:       openai-codex
Model:           gpt-5.3-codex
OAuth:           auth.openai.com (PKCE akisi)
Client ID:       app_EMoamEEZ73f0CkXaXp7hrann
Kapsam:          model.request
Yedek token URL: auth0.openai.com/oauth/token
```

| Islem | Max Token | Sicaklik | Zaman Asimi | Reasoning |
|-------|-----------|----------|-------------|-----------|
| Sorgu plani | 900 | 0.0 | 4sn | Medium |
| Sirket uretimi | 2400 | 0.2 | 10sn | Medium |
| Ilgilik dogrulamasi | 1400 | 0.0 | 6sn | Medium |
| Profil zenginlestirme | 1800 | 0.1 | 18sn | Medium |
| Brief→profil | 700 | 0.1 | — | Medium |
| Brief→profil onarim | 500 | 0.0 | — | Medium |

## 6.2-6.6 Tam LLM Promptlari

(Bolum II ve 2.7'de verildi — gercek prompt metinleri birebir kopyalanmistir)

---

# BOLUM VII — ZAMANLAMA VE OTOMASYON

## 7.1 spawn_sales_scheduler() (satir 12001-12045)

```rust
pub fn spawn_sales_scheduler(kernel: Arc<OpenFangKernel>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(300)).await; // 5 dk bekleme

            let engine = SalesEngine::new(&kernel.config.home_dir);
            if engine.init().is_err() { continue; }

            let profile = match engine.get_profile() {
                Ok(Some(p)) => p,
                _ => continue,
            };

            let now = Local::now();
            // Saat eslesmesi: schedule_hour_local, dakika <= 10
            if now.hour() as u8 != profile.schedule_hour_local
               || now.minute() > 10 {
                continue;
            }

            // Bugun zaten calistirildi mi?
            if engine.already_ran_today(&profile.timezone_mode)
                     .unwrap_or(false) {
                continue;
            }

            info!("Sales scheduler: triggering daily run");
            // 120sn timeout ile calistir
            tokio::time::timeout(
                Duration::from_secs(120),
                engine.run_generation(&kernel)
            ).await;
        }
    });
}
```

**Tetiklenme:** server.rs satir 751'de `sales::spawn_sales_scheduler(kernel.clone())`

## 7.2 Gunluk Cap

```rust
// deliveries_today() — satir 1110
// Tum "sent" teslimatlari sorgular, tarih filtresi uygulama icinde yapilir

// already_ran_today() — satir 1414
// Tum "completed" calismalari sorgular, tarih eslesmesi uygulama icinde
```

## 7.3 Onboarding (satir 8885-8942)

**4 Adim:**
1. **oauth:** `is_codex_oauth_connected(home_dir)` — Codex tokeni var mi?
2. **brief:** `brief.trim().chars().count() >= 20` — Brief 20+ karakter mi?
3. **profile:** `is_profile_ready_for_outbound()` — 6 zorunlu alan dolu mu?
   (product_name, product_description, target_industry, target_geo, sender_name, sender_email)
4. **first_run:** `has_brief && last_successful_run_id.is_some()` — Brief sonrasi basarili calisma var mi?

`completed = tum adimlar done`

---

# BOLUM VIII — KULLANICI ARAYUZU

## 8.1 HTML Yapisi (index_body.html, 508 satir)

**Genel:** Alpine.js SPA, tek sayfa (#prospecting), `x-data="app"` ust seviye + `x-data="salesPage"` icerik

**Yapi:**
```
<body x-data="app">
  <div class="sales-layout">
    <header class="sales-topbar">         // satir 4-22
      Logo + Tema toggle (☼/◯/☾) + Baglanti durumu
    </header>
    <main class="sales-main" x-data="salesPage">  // satir 25-506
      <div x-show="loading">              // Yukleniyor...
      <div x-show="!loading && loadError"> // Hata + Yeniden dene
      <div x-show="!loading && !loadError">
        <div x-show="showOnboarding">     // ONBOARDING (satir 43-212)
          Adim 1: OAuth (satir 69-109)
          Adim 2: Brief (satir 111-130)
          Adim 3: Profil (satir 132-169)
          Adim 4: Ilk run (satir 171-211)
        </div>
        <div x-show="!showOnboarding">    // OPERASYON (satir 214-504)
          Baslik + Yeni run butonu (satir 217-225)
          6 istatistik karti (satir 227-253)
          Run gecmisi tablosu (satir 255-286)
          Prospect profilleri tablosu (satir 288-321)
          Secili account dossier (satir 323-379)
          Mesaj onay kuyrugu (satir 381-419)
          Ham lead tablosu (satir 422-449)
          Secili run profilleri (satir 451-476)
          Teslimatlar tablosu (satir 478-503)
        </div>
      </div>
    </main>
  </div>
</body>
```

## 8.2 JavaScript Veri Modeli (sales.js, 612 satir)

### Tam Durum Yapisi (satir 3-61)

```javascript
function salesPage() {
  return {
    // UI durumlari
    loading: true,
    loadError: '',
    savingProfile: false,
    autofillingProfile: false,
    runningNow: false,
    oauthBusy: false,

    // Brief metni
    profileBrief: '',

    // Profil (JS varsayilanlari — NOT: target_geo = 'TR', Rust'ta 'US')
    profile: {
      product_name: '',
      product_description: '',
      target_industry: '',
      target_geo: 'TR',              // Rust default: 'US' — UYUMSUZLUK
      sender_name: '',
      sender_email: '',
      sender_linkedin: '',
      target_title_policy: 'ceo_then_founder',
      daily_target: 20,
      daily_send_cap: 20,
      schedule_hour_local: 9,
      timezone_mode: 'local'
    },

    // Onboarding durumu
    onboarding: {
      completed: false,
      active_step: 1,
      steps: [],                     // [{key, title, done}, ...]
      oauth_connected: false,
      has_brief: false,
      profile_ready: false,
      first_run_ready: false,
      brief: '',
      last_successful_run_id: ''
    },

    // OAuth durumu
    oauth: {
      connected: false,
      source: '',
      reason: '',
      issued_at: null,
      expires_at: null,
      has_refresh_token: false,
      auth_url: '',
      state: ''
    },

    // Secim ve veriler
    manualCode: '',
    selectedRunId: '',
    selectedProspectId: '',
    runs: [],
    prospects: [],
    runProspects: [],
    leads: [],
    runLeads: [],
    approvals: [],
    deliveries: [],
  };
}
```

### Hesaplanmis Ozellikler (getter'lar)
```javascript
get showOnboarding()       // !onboarding.completed
get pendingApprovals()     // approvals.filter(pending).length
get pendingApprovalItems() // approvals.filter(pending)
get contactReadyProspects() // prospects.filter(contact_ready).length
get companyOnlyProspects() // prospects.filter(company_only).length
```

### Yardimci Fonksiyonlar
```javascript
selectedProspectRecord()   // runProspects + prospects icinde id esle, yoksa ilkini dondur
prospectPrimaryContact(p)  // "isim / unvan" veya "Temas yok / unvan"
prospectChannels(p)        // "email + linkedin" veya "sirket seviyesi"
prospectNextAction(p)      // Durum bazli onerilen sonraki adim
prospectOsintLinks(p)      // linkedin + osint_links, tekilsiz, maks 6
approvalRecipient(a)       // email: payload.to, linkedin: payload.profile_url
approvalTitle(a)           // email: payload.subject, linkedin: "LinkedIn mesaji"
approvalBody(a)            // email: payload.body, linkedin: payload.message
formatDateTime(v)          // new Date(v).toLocaleString('tr-TR')
```

### API Cagrilari
```javascript
// Baslangicta (refreshAll → Promise.all):
GET /api/auth/codex/status
GET /api/sales/profile
GET /api/sales/runs?limit=20
GET /api/sales/prospects?limit=200
GET /api/sales/leads?limit=200
GET /api/sales/approvals?limit=200
GET /api/sales/deliveries?limit=200
GET /api/sales/onboarding/status

// Islemler:
POST /api/auth/codex/start           → popup(560x760), poll 24x2.5sn
POST /api/auth/codex/paste-code      → {code, state}
POST /api/auth/codex/import-cli
POST /api/auth/codex/logout
POST /api/sales/onboarding/brief     → {brief, persist: true}
PUT  /api/sales/profile              → SalesProfile JSON
POST /api/sales/run                  → {} (bos govde)
POST /api/sales/approvals/{id}/approve → {}
POST /api/sales/approvals/{id}/reject  → {reason: 'manual_reject'}
```

### UI Bilesenler
- **Badge siniflar:** badge-success (yesil), badge-warn (sari), badge-error (kirmizi), badge-muted (gri)
- **Prospect durum:** contact_ready → badge-success, contact_identified → badge-warn, diger → badge-muted
- **Run durum:** completed → badge-success, running → badge-warn, failed → badge-error
- **Toast:** OpenFangToast.success/error/warn/info/confirm
- **Tablo:** `.table-wrap` icinde `<table>`, `.sales-line-clamp` ile metin kirpma
- **Onizleme:** `<details class="sales-preview-card">` ile acilir kapaninir mesaj onizlemesi

---

# BOLUM IX — CANLI VERIDE TESPIT EDILEN 12 BUG

## BUG-01: gmail.com Prospect Olarak Kabul Edilmis [KRITIK]

**Belirti:** Prospect listesinde "Gmail / gmail.com" gorunuyor, `info@gmail.com`'a onay bekliyor
**Satir:** Platformder tarayici (satir ~4658-4804), domain cikarma noktasi
**Kok Neden:** Platformder dizininde "Altas Vinc Platform" web sitesi olarak gmail.com kayitli. `is_consumer_email_domain()` (satir 3662) e-posta icin calisiyor ama `fetch_platformder_directory_candidates()` icinde sirket domaini kontrolu yok.
**Cozum:** Dizin tarama ciktisinda `is_consumer_email_domain(domain) || is_blocked_company_domain(domain)` kontrolu ekle.

## BUG-02: info@ E-postalar Filtrelenmeden Lead Olmus [KRITIK]

**Belirti:** 31 onayin ~25'i info@domain.com adreslerine gidiyor
**Satir:** Lead olusturma (satir ~2241-2315), email degiskeni filtrelenmeden kullaniliyor
**Kok Neden:** `email_is_actionable_outreach_email()` (satir 7123) info@ engelliyor AMA dizin taramasindan gelen e-postalar farkli kod yolundan gecip bu filtrelemeyi atliyor. Dogrudan `email` degiskenine ataniyor.
**Cozum:** Lead olusturma oncesinde (satir ~2241) `normalize_actionable_outreach_email()` uygula.

## BUG-03: "Baskan'in Mesaji" Kisi Adi Olarak Cikarilmis [YUKSEK]

**Belirti:** Gocay icin "Baskan'in Mesaji / Founder"
**Satir:** `contact_name_is_placeholder()` (satir 4154-4190)
**Kok Neden:** Turkce sayfa basliklari placeholder listesinde yok
**Cozum:** Listeye ekle: "baskanin mesaji", "genel mudurun mesaji", "hakkimizda", "vizyonumuz", "misyonumuz", "iletisim", "kariyer", "basin"

## BUG-04: Tum Fit Puanlari 100 [YUKSEK]

**Belirti:** 30/32 prospect fit_score=100
**Satir:** `lead_score()` (satir 8631-8640) + cap mantigi (satir ~2251)
**Kok Neden:** `(60 + 20 + aday_puani).min(100)` formulu, info@ bile +20 aldigi icin herkes 100
**Cozum:** Cap kaldir, 0-1000 agirlikli puan, info@ icin +20 degil +5

## BUG-05: Telefon Numaralari Kayboluyor [YUKSEK]

**Belirti:** 0/32 profilde telefon var, dizinlerde mevcut
**Satir:** Lead olusturma satir 2315: `phone: None`
**Kok Neden:** Hardcoded `None`, dizinden cikarilan telefon aktarilmiyor
**Cozum:** `phone: candidate_phone.clone()` veya enrichment'tan aktar

## BUG-06: Cift Onay Kaydi [ORTA]

**Belirti:** Artiplatform icin 2 bekleyen onay (ayni email)
**Satir:** `queue_approvals_for_lead()` (satir 755-792), approvals tablosu (satir 386)
**Kok Neden:** `approvals` tablosunda UNIQUE kisitlama yok
**Cozum:** Onay olusturmadan once `SELECT COUNT(*) FROM approvals WHERE channel=? AND status='pending' AND payload_json LIKE ?` kontrolu

## BUG-07: LinkedIn %0 Basari [YUKSEK]

**Belirti:** 0/32 LinkedIn URL
**Satir:** Arama OSINT icindeki LinkedIn sorgusu
**Kok Neden:** `site:linkedin.com/company "[Turkce]" "[.tr]"` sonuc dondurmuyor
**Cozum:** Yedek: sadece domain, transliterasyon, `site:tr.linkedin.com`

## BUG-08: E-posta Sablonlari Genel [ORTA]

**Belirti:** Tum e-postalar ayni yapi, ayni konu satiri
**Satir:** `build_sales_email_subject()` (satir 5905), `build_sales_email_body()` (satir 5913)
**Kok Neden:** Sabit sablon, LLM kullanilmiyor
**Cozum:** LLM ile sirket-ozel mesaj uretimi

## BUG-09: ICP Sinyalleri Hepsi Ayni [ORTA]

**Belirti:** Tum profillerde ayni "Field service and on-site operations..." metni
**Satir:** Lead olusturma satirlari ~2264-2274
**Kok Neden:** `matched_keywords` bos → `profile.target_industry` yedek
**Cozum:** Kaynak bazli sinyal: dizin adi, site icerigi, bulunan anahtar kelimeler

## BUG-10: "Temas yok" ama "contact_ready" [ORTA]

**Belirti:** "Aksarayvinc — contact_ready — Temas yok"
**Satir:** `prospect_status()` (satir 3210) + JS `prospectPrimaryContact()` (sales.js satir 353-358)
**Kok Neden:** info@ email "actionable" sayildigi icin contact_ready, JS'de isim yoksa "Temas yok"
**Cozum:** "email_only" durumu ekle veya info@ filtreleme

## BUG-11: Pain Point'ler Sablon [DUSUK]

**Belirti:** Tum sirketlerde ayni 3 cumle
**Satir:** `build_prospect_pain_points()` (satir 3367-3403)
**Kok Neden:** Hardcoded sablonlar
**Cozum:** LLM ile sirket-ozel pain point

## BUG-12: Ilk Run Zaman Asimi [DUSUK]

**Belirti:** "exceeded the request timeout"
**Satir:** `run_sales_now()` handler (satir 11761), `SALES_RUN_REQUEST_TIMEOUT_SECS=240`
**Kok Neden:** Ilk run icin 240sn yetersiz
**Cozum:** Arka plan gorevi olarak calistir, API'dan sadece durum sorgula

---

# BOLUM X — HEDEF MIMARI VE YOL HARITASI

## 10.1 Hedef Pipeline Iyilestirmeleri

| Asama | Mevcut | Hedef |
|-------|--------|-------|
| Sorgu plani | Tek dil | Cok dilli (TR+EN) + olay bazli + rakip bazli |
| Kesif | 3 kanal | +LinkedIn sirket arama, +haber/etkinlik |
| Filtreleme | 0-100, cap'li | 0-1000, agirlikli, info@ ayristirmali |
| Zenginlestirme | Site+Arama+LLM | +LinkedIn profil, +telefon aktarimi, +email pattern |
| Mesaj | Sablon | LLM kisisellestirme + A/B varyant |
| Onay | Tekli | Toplu + duzenleme + info@ uyari |
| Teslimat | Email+LinkedIn | +Bounce takip, +otomatik takip (7/14 gun) |

## 10.2 Yeni Puanlama (0-1000)

```
TEMEL (0-200):  Kaynak kalitesi (TMB=80, Web=30, LLM=40)
ILETISIM (0-300): Gercek kisi=80, kisisel email=100, info@=20, LinkedIn=100, tel=50
ICP (0-300):    LLM dogrulama (0.8+=150), sektor esleme=50, site esleme=60
SINYAL (0-200): Site ICP kelime=60, dizin uyelik=40, haber/etkinlik=50
```

---

## SAYISAL OZET

| Metrik | Deger |
|--------|-------|
| Toplam Rust kodu | 14.466 satir (sales.rs) + 1.335 (codex_oauth.rs) |
| JavaScript kodu | 612 satir (sales.js) + 321 (api.js) + 70 (app.js) |
| HTML | 508 satir (index_body.html) |
| Veritabani tablolari | 8 |
| Veritabani indexleri | 5 |
| API endpointleri | 14 satis + 6 OAuth = 20 |
| LLM cagrilari / calisma | 4 (sorgu+kesif+dogrulama+zenginlestirme) |
| LLM modeli | gpt-5.3-codex |
| Turk dizin kaynagi | 8 |
| Sabit | 38 |
| Struct | 8 |
| Tespit edilen bug | 12 (2 kritik, 5 yuksek, 4 orta, 1 dusuk) |
| Iyilestirme maddesi | 23 (6 faz) |


===============================================================
# PARCA 3 — HEDEF TASARIM VE EKRANLAR
===============================================================

# OpenFang Prospecting Engine — Tam Sistem Plani

> Bu dokuman, OpenFang'in B2B prospecting ve lead generation motorunu
> sifirdan anlatan, mevcut sistemi analiz eden, canli verideki buglari
> gosteren, hedef mimariyi tasarlayan ve ekran ekran UI akisini tanimleyen
> kapsamli bir referans dokumanidir.
>
> Hedef kitle: Bu dokumani okuyan LLM'ler (GPT, Gemini, Claude) ve
> insan gelistiriciler. Dokumanin amaci, sistemi her yonuyle anlasilir
> kilmak ve "gercek bir prospecting engine" a donusturmek icin yol
> haritasi sunmaktir.

---

## ICINDEKILER

**KISIM A — MEVCUT DURUM**
1. [Sistem Nedir?](#1-sistem-nedir)
2. [Mimari ve Teknik Altyapi](#2-mimari-ve-teknik-altyapi)
3. [Mevcut Pipeline Akisi](#3-mevcut-pipeline-akisi)
4. [Canli Veride Tespit Edilen 12 Kritik Sorun](#4-canli-veride-tespit-edilen-12-kritik-sorun)

**KISIM B — HEDEF TASARIM**
5. [Hedef Prospecting Pipeline Mimarisi](#5-hedef-prospecting-pipeline-mimarisi)
6. [Asama 1: Akilli Sorgu Planlama](#6-asama-1-akilli-sorgu-planlama)
7. [Asama 2: Cok Kaynakli Paralel Kesif](#7-asama-2-cok-kaynakli-paralel-kesif)
8. [Asama 3: Akilli Filtreleme ve Puanlama](#8-asama-3-akilli-filtreleme-ve-puanlama)
9. [Asama 4: Cok Katmanli Zenginlestirme](#9-asama-4-cok-katmanli-zenginlestirme)
10. [Asama 5: LLM ile Kisisellestirmis Mesaj Uretimi](#10-asama-5-llm-ile-kisisellestirmis-mesaj-uretimi)
11. [Asama 6: Insan-Dongusu Onay Akisi](#11-asama-6-insan-dongusu-onay-akisi)
12. [Asama 7: Cok Kanalli Teslimat ve Takip](#12-asama-7-cok-kanalli-teslimat-ve-takip)

**KISIM C — EKRAN TASARIMLARI**
13. [Dashboard Genel Yapisi](#13-dashboard-genel-yapisi)
14. [Ekran 1: Onboarding Sihirbazi](#14-ekran-1-onboarding-sihirbazi)
15. [Ekran 2: Komuta Merkezi (Ana Pano)](#15-ekran-2-komuta-merkezi-ana-pano)
16. [Ekran 3: Prospect Profil Detay](#16-ekran-3-prospect-profil-detay)
17. [Ekran 4: Mesaj Onay Kuyrugu](#17-ekran-4-mesaj-onay-kuyrugu)
18. [Ekran 5: Teslimat ve Analitik](#18-ekran-5-teslimat-ve-analitik)
19. [Ekran 6: Ayarlar ve Profil Yonetimi](#19-ekran-6-ayarlar-ve-profil-yonetimi)

**KISIM D — TEKNIK ALTYAPI**
20. [Veritabani Semasi](#20-veritabani-semasi)
21. [API Endpoint Haritasi](#21-api-endpoint-haritasi)
22. [LLM Entegrasyonu ve Prompt Muhendisligi](#22-llm-entegrasyonu-ve-prompt-muhendisligi)
23. [Zamanlama ve Otomasyon](#23-zamanlama-ve-otomasyon)
24. [Guvenlik ve Uyumluluk](#24-guvenlik-ve-uyumluluk)

**KISIM E — METRIKLER VE YOL HARITASI**
25. [Basari Metrikleri ve KPI'lar](#25-basari-metrikleri-ve-kpilar)
26. Uygulama Yol Haritasi → bkz Parca 4, Bolum 30

---

# KISIM A — MEVCUT DURUM

## 1. Sistem Nedir?

OpenFang Prospecting Engine, Rust dilinde yazilmis bir B2B potansiyel musteri
bulma ve otomatik iletisim sistemidir. Temel is akisi:

```
Kullanici urununun tanimi (ICP)
    |
    v
Otomatik sirket kesfii (LLM + Web Arama + Turk Dizinleri)
    |
    v
Sirket profilleme ve zenginlestirme (Site OSINT + Arama OSINT + LLM)
    |
    v
Karar verici tespiti (isim, unvan, e-posta, LinkedIn)
    |
    v
Kisisellestirmis mesaj taslagi (e-posta + LinkedIn)
    |
    v
Insan onayi (gonder / reddet)
    |
    v
Otomatik teslimat (SMTP / tarayici otomasyonu)
```

### 1.1 Teknik Konum

- **Dosya:** `crates/openfang-api/src/sales.rs` (~14.466 satir)
- **OAuth:** `crates/openfang-api/src/codex_oauth.rs` (~1.335 satir)
- **UI:** `crates/openfang-api/static/js/pages/sales.js` (~611 satir)
- **Dashboard:** `crates/openfang-api/static/index_body.html` (~508 satir)
- **Veritabani:** `~/.openfang/sales.db` (SQLite, 8 tablo)
- **LLM:** `gpt-5.3-codex` (OpenAI Codex OAuth uzerinden)
- **Zamanlama:** Her 5 dakikada bir kontrol, gunluk 1 calisma (varsayilan saat 09:00)

### 1.2 Mevcut Yetenekler

| Yetenek | Durum | Notlar |
|---------|-------|--------|
| LLM ile sirket kesfii | Calisiyor | gpt-5.3-codex, maks 12 sirket/calisma |
| Web arama kesfii | Calisiyor | Brave/yapili saglayici, 10 sorgu/asama |
| Turk dizin taramasi | Calisiyor | 8 dizin (TMB, ASMUD, MIB, IMDER, ISDER, THBB, EUD, Platformder) |
| Site HTML zenginlestirme | Calisiyor | Ana sayfa + ekip + hakkinda + kariyer |
| Arama OSINT zenginlestirme | Calisiyor | CEO/kurucu arama, LinkedIn arama |
| LLM profil zenginlestirme | Calisiyor | Ozet, pain point, trigger, outreach angle |
| E-posta mesaj taslagi | Calisiyor | Sablon bazli (LLM degil) |
| LinkedIn mesaj taslagi | Calisiyor | Sablon bazli (LLM degil) |
| Insan onay akisi | Calisiyor | Onayla / Reddet butonu |
| E-posta teslimati | Calisiyor | SMTP uzerinden |
| LinkedIn teslimati | Calisiyor | Tarayici otomasyonu |
| Gunluk otomatik calisma | Calisiyor | Cron-benzeri zamanlayici |
| Codex OAuth | Calisiyor | PKCE akisi + token yenileme |

---

## 2. Mimari ve Teknik Altyapi

### 2.1 Sistem Bileşenleri

```
┌─────────────────────────────────────────────────────────┐
│                    KULLANICI ARAYUZU                      │
│  Alpine.js SPA — Tek Sayfa (#prospecting)                │
│  ┌──────────┬──────────┬──────────┬──────────┐          │
│  │Onboarding│Komuta    │Profil    │Mesaj Onay│          │
│  │Sihirbazi │Merkezi   │Detay     │Kuyrugu   │          │
│  └──────────┴──────────┴──────────┴──────────┘          │
└──────────────────────────┬──────────────────────────────┘
                           │ HTTP/JSON API
┌──────────────────────────┼──────────────────────────────┐
│              SALES ENGINE (sales.rs)                      │
│                                                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │Sorgu Plani  │  │Paralel      │  │LLM Ilgilik  │     │
│  │(LLM/Sezgisel)│  │Kesif Motoru │  │Dogrulamasi  │     │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘     │
│         │                │                │              │
│  ┌──────▼────────────────▼────────────────▼──────┐      │
│  │          Birlestirme & Tekilsizlestirme        │      │
│  └──────────────────────┬────────────────────────┘      │
│                         │                                │
│  ┌──────────────────────▼────────────────────────┐      │
│  │  Zenginlestirme (Site + Arama + LLM)           │      │
│  └──────────────────────┬────────────────────────┘      │
│                         │                                │
│  ┌──────────────────────▼────────────────────────┐      │
│  │  Lead Olusturma + Mesaj Taslagi + Onay Kuyrugu │      │
│  └───────────────────────────────────────────────┘      │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────┼──────────────────────────────┐
│              VERI KATMANI                                 │
│  sales.db (SQLite)                                       │
│  ┌──────────┬───────┬──────────┬────────────────┐       │
│  │sales_runs│leads  │approvals │prospect_profiles│       │
│  │sales_    │deliver│discovered│sales_onboarding │       │
│  │profile   │ies    │_domains  │                 │       │
│  └──────────┴───────┴──────────┴────────────────┘       │
└─────────────────────────────────────────────────────────┘
```

### 2.2 Veri Akis Sirasi (Mevcut)

```
[Kullanici ICP tanimlar]
         │
         ▼
[1] llm_build_lead_query_plan()          ← 4sn zaman asimi
         │ Basarisizsa: heuristic_lead_query_plan()
         ▼
[2] tokio::join! {                       ← 3 paralel akis
      llm_generate_company_candidates()  ← 10sn, maks 12 sirket
      discover_via_web_search()          ← 8sn, 10 sorgu
      fetch_free_discovery_candidates()  ← 3.2sn, 8 Turk dizini
    }
         │
         ▼
[3] merge_all_discovery_sources()        ← Domain bazli birlestirme
         │
         ▼
[4] llm_validate_candidate_relevance()   ← 6sn, maks 40 aday
         │
         ▼
[5] Filtreleme + Siralama                ← score >= MIN + engellenenler
         │
         ▼
[6] seed_prospect_profiles_for_run()     ← LLM zenginlestirme, 18sn
         │
         ▼
[7] Lead Uretim Dongusu                  ← Her aday icin:
    ├── Web iletisim arama
    ├── Site HTML zenginlestirme
    ├── Normalizasyon & birlestirme
    ├── Lead olusturma (UNIQUE kisitlama)
    └── Onay kuyruguna ekleme
```

### 2.3 Temel Sabitler

| Sabit | Deger | Aciklama |
|-------|-------|----------|
| `SALES_LLM_MODEL` | `gpt-5.3-codex` | Kullanilan LLM modeli |
| `MIN_DOMAIN_RELEVANCE_SCORE` | 5 | Minimum aday puani |
| `MAX_DISCOVERY_QUERIES` | 10 | Asama basi web arama |
| `MAX_DIRECT_ENRICH_ATTEMPTS` | 12 | Site HTML indirme limiti |
| `MAX_WEB_CONTACT_SEARCH_ATTEMPTS` | 12 | OSINT arama limiti |
| `DIRECT_ENRICH_TIMEOUT_MS` | 3500 | Site indirme zaman asimi |
| `SITE_PAGE_FETCH_TIMEOUT_MS` | 1600 | Sayfa indirme zaman asimi |
| `MAX_OSINT_LINKS_PER_PROSPECT` | 6 | Profil basi kanit baglantisi |
| `MAX_FREE_DIRECTORY_CANDIDATES` | 30 | Turk dizini toplam aday |
| `SALES_RUN_REQUEST_TIMEOUT_SECS` | 240 | API zaman asimi |
| `SALES_SEARCH_BATCH_CONCURRENCY` | 3 | Arama toplu esanliligi |
| `SALES_OSINT_PROFILE_CONCURRENCY` | 4 | OSINT profil esanliligi |

### 2.4 Turk Dizin Kaynaklari

| Dizin | URL | Sektor | Kosul | Maks Aday |
|-------|-----|--------|-------|-----------|
| TMB | tmb.org.tr/en/members | Muteahhitler | field_ops | 8 |
| EUD | eud.org.tr/en/members | Enerji | energy | 6 |
| ASMUD | asmud.org.tr/Uyeler.asp | Asfalt/yol | field_ops | 10 |
| Platformder | platformder.org.tr/rehber | Platform kiralama | field_ops | 10 |
| MIB | mib.org.tr (9 sayfa) | Madencilik/makine | field_ops | 10 |
| IMDER | imder.org.tr | Makine sanayi | field_ops | 8 |
| ISDER | isder.org.tr | ISG ekipman | field_ops | 8 |
| THBB | thbb.org.tr | Insaat malzeme | field_ops | 8 |

---

## 3. Mevcut Pipeline Akisi

### 3.1 SalesProfile — Kullanici Yapilandirmasi

```rust
SalesProfile {
    product_name: String,           // "Machinity"
    product_description: String,    // "AI teammate that manages..."
    target_industry: String,        // "Field service and on-site operations..."
    target_geo: String,             // "TR"
    sender_name: String,            // "Machinity"
    sender_email: String,           // "hello@machinity.com"
    sender_linkedin: Option<String>,
    target_title_policy: String,    // "ceo_then_founder"
    daily_target: u32,              // 20
    daily_send_cap: u32,            // 20
    schedule_hour_local: u8,        // 9
    timezone_mode: String,          // "local"
}
```

### 3.2 Prospect Profil Yapisi

```rust
SalesProspectProfile {
    id: String,                         // company_domain
    run_id: String,
    company: String,
    website: String,
    company_domain: String,
    fit_score: i32,                     // 0-100
    profile_status: String,             // contact_ready | contact_identified | company_only
    summary: String,                    // "{company} fits via {signals}. {contact}. {channels}."
    matched_signals: Vec<String>,
    primary_contact_name: Option<String>,
    primary_contact_title: Option<String>,
    primary_email: Option<String>,
    primary_linkedin_url: Option<String>,
    company_linkedin_url: Option<String>,
    osint_links: Vec<String>,           // Maks 6
    contact_count: u32,
    source_count: u32,
    buyer_roles: Vec<String>,           // Sablon: CEO/Founder | Head of Operations | ...
    pain_points: Vec<String>,           // Sablon: 3 sabit cumle
    trigger_events: Vec<String>,        // Sablon: 3 sabit cumle
    recommended_channel: String,        // "email" veya "linkedin"
    outreach_angle: String,             // Sablon bazli
    research_status: String,            // "heuristic" veya "llm_enriched"
    research_confidence: f32,           // 0.0-1.0
}
```

### 3.3 Lead Yapisi

```rust
SalesLead {
    id: String,                     // UUID
    run_id: String,
    company: String,
    website: String,
    company_domain: String,
    contact_name: String,           // Gercek isim veya "Leadership Team"
    contact_title: String,          // "CEO/Founder" veya "Chairman"
    linkedin_url: Option<String>,
    email: Option<String>,
    phone: Option<String>,          // BUG: Her zaman None (hardcoded)
    reasons: Vec<String>,           // 3-4 sablon cumle
    email_subject: String,          // "{company} icin saha operasyon koordinasyonu"
    email_body: String,             // Sablon bazli Turkce/Ingilizce
    linkedin_message: String,       // Sablon bazli, maks 300 karakter
    score: i32,                     // lead_score + candidate.score, cap 100
    status: String,
}
```

### 3.4 Mevcut E-posta Sablonu

**Turkce (target_geo icinde "TR"):**
```
Konu: {company} icin saha operasyon koordinasyonu

Merhaba {alici},

{company} ile ilgili su sinyali gordum: {kanit_kisa}.

{company} tarafinda {eslesen_anahtar} operasyonlarinda is atama, takip
ve gecikme yonetimi kolayca daginik hale gelebiliyor. {urun_adi} burada
su ise yarayabilir: {urun_aciklama_kisa}.

Uygunsa {company} icin 3 maddelik kisa bir operasyon akisi onerisi
paylasabilirim.

Selamlar,
{gonderen_adi}
```

**Sorunlar:**
- Tum sirketlere ayni sablon → robotik
- Sirket-ozel aci noktasi yok
- Urun aciklamasi ortada kesilebiliyor (truncation)
- Kisi adi yoksa "Merhaba {sirket_adi}" yaziliyor

### 3.5 Mevcut Puanlama Sistemi

```
lead_score():
  Temel:     60
  + E-posta: 20 (varsa)
  + LinkedIn: 20 (varsa)

Nihai: (lead_score + candidate.score).min(100)

Sorun: Neredeyse herkes 100 aliyor cunku:
  60 + 20 (info@ bile sayiliyor) + 20+ (dizin puani) = 100
```

### 3.6 Mevcut Durum Siniflandirmasi

```rust
fn prospect_status(name, email, linkedin) -> &str {
    if email.is_actionable() || linkedin.is_valid() {
        "contact_ready"       // Yesil rozet
    } else if name.is_real_person() {
        "contact_identified"  // Mavi rozet
    } else {
        "company_only"        // Sari rozet
    }
}

Sorun: info@ e-postalar "actionable" sayildigi icin
  kisi adi olmayan profiller bile "contact_ready" oluyor
```

---

## 4. Canli Veride Tespit Edilen 12 Kritik Sorun

26 Mart 2026 tarihli 3 calismanin canli ciktisi analiz edildi:

### BUG-01: gmail.com Prospect Olarak Kabul Edilmis [KRITIK]

**Belirti:** "Gmail" isimli prospect, gmail.com domaini ile kuyrukta.
`info@gmail.com`'a e-posta gonderimi bekliyor.

**Kok Neden:** Platformder dizininde "Altas Vinc Platform" web sitesi olarak
`gmail.com` kayitli (muhtemelen e-posta adresi). `is_consumer_email_domain()`
e-posta icin calisiyor ama `sirket domaini` olarak gecmesine izin veriyor.

**Etki:** Spam gonderimi, domain itibar kaybii.

**Cozum:** Dizin tarama ciktisinda da consumer domain kontrolu uygula.

---

### BUG-02: ~25 Adet info@ E-posta Filtrelenmeden Lead Olmus [KRITIK]

**Belirti:** Onay kuyugundaki 31 kaydin ~25'i info@domain.com adreslerine gidiyor.

**Kok Neden:** `email_is_generic_role_mailbox()` fonksiyonu "info" onekini
engelliyor AMA dizin taramasi sonucu elde edilen e-postalar farkli bir kod
yolundan gecip bu filtrelemeyi atliyor.

**Etki:** <%1 yanit orani, spam riski, domain itibar kaybii.

**Cozum:** Lead olusturma noktasinda tum e-postalari
`normalize_actionable_outreach_email()` filtresinden gecir.

---

### BUG-03: "Baskan'in Mesaji" Kisi Adi Olarak Cikarilmis [YUKSEK]

**Belirti:** Gocay.com.tr icin birincil temas: "Baskan'in Mesaji / Founder"

**Kok Neden:** Site HTML taramasi sayfa bolum basligini kisi adi olarak cikarmis.
`contact_name_is_placeholder()` listesinde Turkce karsilik yok.

**Cozum:** Turkce sayfa basliklarini placeholder listesine ekle:
"baskanin mesaji", "genel mudurun mesaji", "hakkimizda", "vizyonumuz", "misyonumuz"

---

### BUG-04: Tum Fit Puanlari 100 — Siralamada Ayricilik Yok [YUKSEK]

**Belirti:** 32 profilden 30'u fit_score=100. Alarko (dev holding) ile
Aksarayvinc (kucuk isletme) ayni puan.

**Kok Neden:** `(60 + 20 + aday_puani).min(100)` formulu, info@ bile
e-posta sayildigi icin neredeyse herkesi 100'e cikariyor.

**Cozum:** 0-100 cap'ini kaldir, agirlikli puanlama sistemi tasarla.

---

### BUG-05: Telefon Numaralari Cikarilip Kaybediliyor [YUKSEK]

**Belirti:** Hicbir profilde telefon yok. Ama dizinlerde telefon mevcut
(orn: "0530 851 89 61", "+90 262 679 56 00").

**Kok Neden:** Dizin tarama kodu telefonu cikarir ama Lead olusturmada
`phone: None` hardcoded (satir 2315).

**Cozum:** Cikarilan telefon numarasini Lead struct'ina aktar.

---

### BUG-06: Ayni E-postaya Cift Onay Kaydi [ORTA]

**Belirti:** `kiralama@artiplatform.com.tr` icin 2 farkli run'dan 2 onay.

**Kok Neden:** `leads` tablosu UNIQUE kisitlamali ama `approvals` tablosu degil.

**Cozum:** Onay olusturmadan once ayni kanal+alici+pending kontrolu ekle.

---

### BUG-07: LinkedIn Zenginlestirme Turk Sirketlerinde %0 [YUKSEK]

**Belirti:** 32 profilden 0 tanesi LinkedIn URL'sine sahip.

**Kok Neden:** `site:linkedin.com/company "[Turkce Ad]" "[domain.tr]"` sorgusu
Turkce karakterler ve kucuk sirketler icin sonuc dondurmuyor.

**Cozum:** Yedek arama stratejileri: sadece domain, transliterasyon, genel arama.

---

### BUG-08: E-posta Sablonlari Genel, LLM Kullanilmiyor [ORTA]

**Belirti:** Tum e-postalar ayni yapiyla basliyor: "Merhaba, {sirket} ile ilgili
su sinyali gordum..."

**Kok Neden:** `build_sales_email_body()` sabit sablon kullaniyor, LLM yok.

**Cozum:** LLM ile sirket-ozel e-posta govdesi ve konu satiri uret.

---

### BUG-09: ICP Sinyalleri Hepsi Ayni Metin [ORTA]

**Belirti:** Tum profillerde: "Field service and on-site operations teams:
maintenance, installation, facility management..."

**Kok Neden:** `matched_keywords` bos oldugunda `profile.target_industry` yedek
kullaniliyor. Tum adaylar ayni profildeki ayni degeri aliyor.

**Cozum:** Kaynak bazli sinyal farklistirma (dizin adi, site icerigi, vs.)

---

### BUG-10: "Temas yok" ama "contact_ready" [ORTA]

**Belirti:** "Aksarayvinc — contact_ready — Temas yok — 1 temas — email"

**Kok Neden:** info@ e-posta "actionable" sayildigi icin kisi adi olmadan
"contact_ready" durumu veriliyor.

**Cozum:** Yeni ara durum: "email_only" veya info@ filtreleme.

---

### BUG-11: Pain Point'ler Tum Sirketler Icin Ayni [DUSUK]

**Belirti:** Her sirket icin ayni 3 sablon cumle.

**Kok Neden:** `build_prospect_pain_points()` hardcoded sablonlar kullaniyor.

**Cozum:** LLM ile sirket-ozel pain point uretimi.

---

### BUG-12: Ilk Run Zaman Asimi [DUSUK]

**Belirti:** "Prospecting run exceeded the request timeout..."

**Kok Neden:** 240sn API zaman asimi ilk run icin yetersiz kalabiliyor.

**Cozum:** Arka plan gorevi olarak calistir, API'dan sadece durum sorgula.

---

# KISIM B — HEDEF TASARIM

## 5. Hedef Prospecting Pipeline Mimarisi

Mevcut 7 asamali pipeline'i 7 asamada yeniden tasarliyoruz. Her asama
mevcut bugları cozer ve yeni yetenekler ekler.

```
┌──────────────────────────────────────────────────────────┐
│                HEDEF PIPELINE AKISI                       │
│                                                           │
│  [1] AKILLI SORGU        LLM + sezgisel + sektor ozel   │
│       PLANLAMA           sorgu seti uretimi               │
│           │                                               │
│  [2] COK KAYNAKLI        LLM Sirket Uretimi             │
│       PARALEL KESIF      Web Arama (Brave+Tavily)        │
│           │               Turk Dizin Taramasi             │
│           │               LinkedIn Sirket Arama (YENi)    │
│           │               CrunchBase/Builtwith (YENi)     │
│           │                                               │
│  [3] AKILLI              Consumer domain engeli           │
│       FILTRELEME         Agirlikli puanlama (0-1000)      │
│       & PUANLAMA         LLM ilgilik dogrulamasi          │
│           │               Sirket buyukluk tahmini          │
│           │                                               │
│  [4] COK KATMANLI        Site HTML (coklu sayfa)         │
│       ZENGINLESTIRME     Arama OSINT (coklu sorgu)       │
│           │               LinkedIn Profil Arama (YENi)    │
│           │               Telefon cikarma (DUZELTME)      │
│           │               E-posta pattern tahmini (YENi)  │
│           │               LLM Profil Arastirma            │
│           │                                               │
│  [5] LLM MESAJ           Sirket-ozel konu satiri          │
│       URETIMI            Kisisellestirmis govde           │
│           │               Turkce/Ingilizce otomatik        │
│           │               A/B varyant uretimi (YENi)      │
│           │                                               │
│  [6] INSAN-DONGUSU       Onizleme + duzenleme            │
│       ONAY               Toplu onay/red                   │
│           │               Cift kayit engeli                │
│           │               Kalite puani gosterimi           │
│           │                                               │
│  [7] COK KANALLI         E-posta (SMTP)                  │
│       TESLIMAT           LinkedIn (tarayici)              │
│       & TAKIP            Bounce/yanit takibi (YENi)       │
│                           Gunluk rapor (YENi)             │
└──────────────────────────────────────────────────────────┘
```

---

## 6. Asama 1: Akilli Sorgu Planlama

### 6.1 Mevcut Durum
- LLM ile sorgu plani olusturuyor (4sn zaman asimi)
- Basarisiz olursa sezgisel yedek
- Tek dil (Ingilizce sorgular)

### 6.2 Hedef Tasarim

```
Girdi: SalesProfile (urun, sektor, cografya)
         │
         ▼
┌─────────────────────────────┐
│  LLM Sorgu Planlayici       │
│                              │
│  Istem:                      │
│  "Sen B2B pazar arastirmaci- │
│  sisin. Asagidaki urun ve   │
│  ICP icin 3 katmanli arama  │
│  stratejisi olustur:         │
│                              │
│  Katman 1: Sektor anahtar   │
│    kelime sorgulari (10)     │
│  Katman 2: Rakip/alternatif │
│    kullanici sorgulari (5)   │
│  Katman 3: Olay bazli       │
│    sorgular (5)              │
│    (yeni fabrika, ihale,     │
│     yeni sube, iso sertif.)  │
│                              │
│  Dil: Hem Turkce hem Ingil.  │
│  Disarida birak: [...]       │
│  Hedef unvanlar: [...]       │
│  Hedef sirket buyuklugu:     │
│    20-5000 calisan            │
│                              │
│  Cikti: JSON                 │
└──────────────┬──────────────┘
               │
               ▼
         LeadQueryPlan {
           discovery_queries: Vec<String>,     // 20 sorgu
           must_include_keywords: Vec<String>,
           exclude_keywords: Vec<String>,
           contact_titles: Vec<String>,
           competitor_names: Vec<String>,       // YENi
           event_queries: Vec<String>,          // YENi
           languages: Vec<String>,              // ["tr", "en"]
         }
```

### 6.3 Yenilikler
- **Cok dilli sorgular:** Turkce sirketler icin Turkce anahtar kelimeler
- **Olay bazli sorgular:** "ihale kazandi", "yeni tesis", "ISO sertifikasi"
- **Rakip bazli sorgular:** Mevcut cozum kullanan sirketler

---

## 7. Asama 2: Cok Kaynakli Paralel Kesif

### 7.1 Mevcut Kaynaklar (Korunacak)
1. LLM sirket uretimi (gpt-5.3-codex)
2. Web arama (Brave / yapili saglayici)
3. Turk dizin taramasi (8 dizin)

### 7.2 Yeni Kaynaklar (Eklenecek)

**LinkedIn Sirket Arama:**
```
Sorgu: site:linkedin.com/company "[sektor]" "[cografya]"
Amac: LinkedIn sirket sayfalarindan ICP'ye uyan sirketleri bul
Yedek: domain-only arama, translitere isim arama
```

**Haber/Etkinlik Arama:**
```
Sorgu: "[sektor] [cografya] ihale kazandi 2026"
       "[sektor] yeni tesis acilisi"
Amac: Aktif olarak buyuyen / degisim yasayan sirketleri bul
```

### 7.3 Dizin Tarama Iyilestirmeleri

**Consumer Domain Engeli (BUG-01 Duzeltmesi):**
```rust
// Her dizin tarama fonksiyonunda domain cikarildiktan sonra:
fn is_valid_company_domain(domain: &str) -> bool {
    !is_consumer_email_domain(domain)
    && !is_blocked_company_domain(domain)
    && !domain.ends_with(".gov.tr")
    && domain.contains('.')
    && domain.len() > 4
}
```

**Telefon Cikarma Aktarimi (BUG-05 Duzeltmesi):**
```rust
// FreeDiscoveryCandidate'e phone alani ekle:
struct FreeDiscoveryCandidate {
    domain: String,
    company_name: String,
    contact_hints: Vec<ContactHint>,
    phone: Option<String>,       // YENi — dizinden cikarilan telefon
    source: String,
}
```

---

## 8. Asama 3: Akilli Filtreleme ve Puanlama

### 8.1 Yeni Puanlama Sistemi (BUG-04 Duzeltmesi)

Mevcut 0-100 skalasi yerine **0-1000 agirlikli puanlama:**

```
TEMEL PUAN (0-200):
  Dizin kaynagi kalitesi:
    TMB/ASMUD uye:         +80  (dogrulanmis uyelik)
    IMDER/ISDER uye:        +70
    Platformder kayitli:    +50
    Web arama sonucu:       +30
    LLM uretimli:           +40

ILETISIM KALITESI (0-300):
  Gercek kisi adi:          +80
  Kisisel e-posta:          +100 (ali@sirket.com)
  Genel e-posta (info@):    +20  (BUG-02: eskiden +100 idi)
  LinkedIn profil URL:      +100
  Telefon numarasi:         +50
  Birden fazla iletisim:    +30

ICP UYUMU (0-300):
  LLM ilgilik dogrulamasi:
    confidence >= 0.8:      +150
    confidence >= 0.5:      +80
    confidence < 0.5:       +0
  Sektor anahtar kelime:    +50
  Cografya eslesmesi:       +40
  Site icerik eslesmesi:    +60

SINYAL KALITESI (0-200):
  Site'ta ICP anahtar kel.: +60
  Dizin uyelik aciklamasi:  +40
  Haber/etkinlik sinyali:   +50
  Buyume gostergesi:        +50
```

### 8.2 info@ E-posta Siniflandirmasi (BUG-02 + BUG-10 Duzeltmesi)

```
E-posta tipleri ve puanlari:

  kisi@sirket.com    → "personal_email"    → +100 puan, contact_ready
  info@sirket.com    → "generic_email"     → +20 puan,  email_only (YENi durum)
  support@sirket.com → "role_email"        → +10 puan,  company_only
  yok                → null                → +0 puan,   company_only
```

### 8.3 Yeni Prospect Durum Siniflandirmasi

```
contact_ready:       Kisisel e-posta VEYA LinkedIn profili (gercek kisi)
email_only:          Sadece genel e-posta (info@), kisi adi yok — YENi
contact_identified:  Kisi adi var ama kanal yok
company_only:        Sadece sirket bilgisi
blocked:             Gecersiz domain veya kalici engel — YENi
```

---

## 9. Asama 4: Cok Katmanli Zenginlestirme

### 9.1 Katman 1: Site HTML Zenginlestirme (Mevcut, Iyilestirmeli)

```
Hedef sayfalar: / , /team , /about , /hakkimizda , /ekibimiz , /yonetim
Cikarilacaklar:
  - Kisi adlari (placeholder filtrelemeli — BUG-03 duzeltmesi)
  - Unvanlar
  - E-posta adreslari (info@ filtrelemeli)
  - Telefon numaralari (YENi: Lead'e aktarilacak)
  - LinkedIn profil URL'leri
  - Sirket aciklama metni

Placeholder genisletme (BUG-03):
  Mevcut: "Leadership Team", "Board of Directors" vb.
  Eklenen: "Başkanın Mesajı", "Genel Müdürün Mesajı",
           "Hakkımızda", "Vizyonumuz", "Misyonumuz",
           "İletişim", "Kariyer", "Basın"
```

### 9.2 Katman 2: Arama OSINT Zenginlestirme (Mevcut, Iyilestirmeli)

```
Sorgular:
  1. "{sirket} CEO site:linkedin.com"
  2. "{sirket} kurucu"
  3. "{sirket} genel mudur"
  4. "{domain}" site:linkedin.com/in/       ← YENi: kisi profili
  5. "{domain}" site:linkedin.com/company/  ← YENi: sirket sayfasi
  6. "{sirket} {sektor} email"              ← YENi: e-posta pattern

LinkedIn arama yedek stratejisi (BUG-07 duzeltmesi):
  Ilk:   site:linkedin.com/company "{Turkce Ad}" "{domain}"
  Yedek1: site:linkedin.com/company "{domain_adi}"
  Yedek2: "{sirket}" linkedin
  Yedek3: site:tr.linkedin.com/company "{sirket}"
```

### 9.3 Katman 3: E-posta Pattern Tahmini (YENi)

```
Eger kisi adi biliniyorsa ama e-posta yoksa:

  Kisi: "Ali Vural", Domain: "abc.com.tr"

  Tahmin edilen e-postalar (dogrulama sirasyla):
    1. ali.vural@abc.com.tr
    2. a.vural@abc.com.tr
    3. avural@abc.com.tr
    4. ali@abc.com.tr

  Dogrulama: MX kayit kontrolu + SMTP VRFY (opsiyonel)
```

### 9.4 Katman 4: LLM Profil Arastirma (Mevcut, Iyilestirmeli)

```
Mevcut sablon bazli uretim YERINE gercek LLM arastirma:

Istem:
  "Asagidaki sirket hakkinda kisa bir arastirma raporu yaz:

  Sirket: {company}
  Domain: {domain}
  Sektor: {target_industry}
  Site icerigi: {site_excerpt}
  Bulunan kisiler: {contacts}

  Asagidaki formatta JSON dondur:
  {
    summary: 'Sirketin 2-3 cumlelik ozeti',
    pain_points: ['Sirket-ozel aci noktasi 1', '...'],
    trigger_events: ['Gercek tetikleyici olay 1', '...'],
    buyer_roles: ['Uygun alici rolu 1', '...'],
    outreach_angle: 'Bu sirkete ozel satis acisi',
    company_size_estimate: 'SMB | mid-market | enterprise',
    recommended_channel: 'email | linkedin | phone'
  }"

Bu sekilde her sirket icin OZEL icerik uretilir.
Mevcut sablon bazli build_prospect_pain_points() KALDIRILIR.
```

---

## 10. Asama 5: LLM ile Kisisellestirmis Mesaj Uretimi

### 10.1 Mevcut Durum (Sablon Bazli)
```
Konu: "{company} icin saha operasyon koordinasyonu"
Govde: Sabit sablon, degisken olarak sirket adi ve kanit parcasi
```

### 10.2 Hedef Tasarim (LLM Bazli)

```
LLM Mesaj Uretim Istemi:

"Sen uzman bir B2B satis yazarisin. Asagidaki bilgilere dayanarak
KISA ve ETKILI bir soguk e-posta yaz.

KURALLAR:
- Maks 120 kelime
- Ilk cumle: sirket-ozel gancho (dikkat cekici)
- Ikinci paragraf: tek aci noktasi + cozum baglantisi
- Ucuncu paragraf: dusuk-bariyer CTA (soru sor, kaynak paylas)
- Imza satiri: gonderen adi
- Konu satiri: maks 50 karakter, merak uyandirici
- DIL: {turkce veya ingilizce}

SIRKET BILGISI:
  Sirket: {company}
  Sektor: {industry}
  Karar verici: {contact_name} ({contact_title})
  Aci noktasi: {pain_point_1}
  Tetikleyici: {trigger_event_1}
  Kanit: {evidence}

URUN:
  Ad: {product_name}
  Deger onerisi: {product_description}

JSON CIKTI:
{
  email_subject: '...',
  email_body: '...',
  linkedin_message: '... (maks 280 karakter)',
  email_subject_variant_b: '...',   // A/B test icin
  email_body_variant_b: '...'        // A/B test icin
}"
```

### 10.3 Dil Secimi Mantigi

```
if target_geo contains "TR" AND contact on Turkish domain:
    dil = "Turkce"
elif contact_name looks Turkish (Turkce karakter iceriyorsa):
    dil = "Turkce"
else:
    dil = "English"
```

---

## 11. Asama 6: Insan-Dongusu Onay Akisi

### 11.1 Mevcut Sorunlar
- Cift kayit engeli yok (BUG-06)
- info@ e-postalar kuyruğa giriyor
- Onizleme yetersiz — mesaj icerigi gizli
- Toplu onay/red yok

### 11.2 Hedef Onay Akisi

```
Lead Olusturuldu
    │
    ▼
[Kalite Kontrolu]
    ├── info@ e-posta? → Durum: "email_only", onay kuyuguna EKLEME
    ├── gmail.com domain? → ENGELLE, atla
    ├── Ayni alici+kanal bekliyor mu? → ENGELLE, cift kayit
    ├── Gunluk kap doldu mu? → Kuyrukta tut, ertesi gun
    │
    ▼
[Onay Kuyuguna Ekle]
    │
    ▼
[Kullanici Panelde Gorur]
    ├── Tam mesaj onizlemesi (genisletilebilir)
    ├── Prospect profil ozeti (yan panel)
    ├── Kalite puani gosterimi (0-1000)
    ├── Oneilen aksiyon: "Onayla" (yuksek puan) / "Gozden gecir" (dusuk puan)
    │
    ├── [Onayla] → send_email() veya send_linkedin()
    ├── [Duzenle + Onayla] → Mesaji duzenle, sonra gonder
    ├── [Reddet] → Neden sec (yanlis kisi / yanlis sirket / spam riski)
    └── [Toplu Onayla] → Secili tum yuksek puanli mesajlari gonder
```

---

## 12. Asama 7: Cok Kanalli Teslimat ve Takip

### 12.1 Mevcut Kanallar
- E-posta (SMTP)
- LinkedIn (tarayici otomasyonu)

### 12.2 Hedef Kanallar

| Kanal | Oncelik | Mekanizma | Durum |
|-------|---------|-----------|-------|
| E-posta (SMTP) | Birincil | Dogrudan SMTP gonderim | Mevcut |
| LinkedIn Mesaj | Ikincil | Tarayici otomasyonu | Mevcut |
| Telefon hatirlatma | Ucuncul | Dashboard'da gosterim (el ile arama) | YENi |
| WhatsApp | Gelecek | WhatsApp Business API | Planli |

### 12.3 Teslimat Sonrasi Takip (YENi)

```
Teslimat tamamlandi
    │
    ▼
[3 gun sonra]
  E-posta bounce mu? → Durumu "bounced" olarak guncelle
  Yanit geldi mi? → Durumu "replied" olarak guncelle
  Acildi mi? (tracking pixel) → Durumu "opened" olarak guncelle
    │
    ▼
[7 gun sonra — yanit yoksa]
  Otomatik takip e-postasi taslagi olustur
  Onay kuyuguna ekle ("follow_up" tipi)
    │
    ▼
[14 gun sonra — hala yanit yoksa]
  Farkli kanal dene (e-posta → LinkedIn veya tersi)
  Onay kuyuguna ekle ("channel_switch" tipi)
```

---

# KISIM C — EKRAN TASARIMLARI

## 13. Dashboard Genel Yapisi

### 13.1 Teknik Altyapi
- **Framework:** Alpine.js (hafif reaktif)
- **Stil:** CSS degiskenleri ile tema (acik/karanlik/sistem)
- **Navigasyon:** Tek sayfa, bolum bazli kaydirma + sekme navigasyonu
- **Veri:** REST API ile JSON, 5 saniyede bir yoklama
- **Bildirim:** Toast sistemi (basari/hata/uyari/bilgi)
- **Dil:** Turkce UI (target_geo=TR icin)

### 13.2 Genel Duzen

```
┌──────────────────────────────────────────────────────────┐
│  [Logo] OpenFang Prospecting Harness        [☼][◯][☾]   │
│                                           [Baglanti:●]   │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  Onboarding tamamlanmadiysa:                             │
│    → Onboarding Sihirbazi (4 adim)                       │
│                                                           │
│  Onboarding tamamlandiysa:                               │
│    → Sekme Navigasyonu:                                  │
│       [Komuta Merkezi] [Profiller] [Onay] [Teslimat]     │
│    → Secili sekmenin icerigi                             │
│                                                           │
└──────────────────────────────────────────────────────────┘
```

---

## 14. Ekran 1: Onboarding Sihirbazi

### 14.1 Ilerleme Cubugu

```
┌──────────────────────────────────────────────────────────┐
│  Prospecting Harness Kurulumu                             │
│                                                           │
│  [1 OAuth ●]──[2 Brief ○]──[3 Profil ○]──[4 İlk Run ○] │
│                                                           │
└──────────────────────────────────────────────────────────┘
```

### 14.2 Adim 1: Codex OAuth

```
┌──────────────────────────────────────────────────────────┐
│  ADIM 1: LLM Baglantisi                                  │
│                                                           │
│  Prospecting motoru yuksek kaliteli sirket kesfii icin    │
│  Codex LLM'i kullanir. Baglanti kurmak icin:             │
│                                                           │
│  Durum: [● Bagli] veya [○ Bekleniyor]                    │
│                                                           │
│  ┌────────────────────┐  ┌────────────────────┐          │
│  │  OAuth ile Baglan   │  │ CLI'dan Aktar      │          │
│  └────────────────────┘  └────────────────────┘          │
│                                                           │
│  Veya manuel kod giris: [________________] [Gonder]       │
│                                                           │
│  [Bagli ise: Hesap bilgisi + Cikis Yap butonu]           │
└──────────────────────────────────────────────────────────┘
```

### 14.3 Adim 2: Sirket Briefi

```
┌──────────────────────────────────────────────────────────┐
│  ADIM 2: Sirketinizi Anlatin                              │
│                                                           │
│  Urununu, hedef musterini ve sektoru anlatan kisa bir     │
│  metin yazin. Bu metin profil alanlarini otomatik          │
│  doldurmak icin kullanilacak.                              │
│                                                           │
│  ┌──────────────────────────────────────────────────┐    │
│  │ Machinity, saha operasyonlarini yoneten bir AI    │    │
│  │ takim arkadasi. Toplanti aksiyonlarini otomatik   │    │
│  │ yakalar, gorev atar, WhatsApp/Slack ile takip     │    │
│  │ eder. Hedef: Turkiye'deki insaat, bakim-onarim,   │    │
│  │ tesis yonetimi ve saha servis sirketleri.         │    │
│  │ (min 20 karakter)                                  │    │
│  └──────────────────────────────────────────────────┘    │
│                                                           │
│  [Brieften Otomatik Doldur →]                             │
└──────────────────────────────────────────────────────────┘
```

### 14.4 Adim 3: Profil Dogrulama

```
┌──────────────────────────────────────────────────────────┐
│  ADIM 3: Profilinizi Dogrulayin                           │
│                                                           │
│  ┌─────────────────────┬─────────────────────┐           │
│  │ Urun Adi            │ Hedef Sektor        │           │
│  │ [Machinity        ] │ [Field service... ] │           │
│  ├─────────────────────┼─────────────────────┤           │
│  │ Hedef Cografya      │ Unvan Politikasi    │           │
│  │ [TR               ] │ [CEO sonra Founder▼]│           │
│  ├─────────────────────┼─────────────────────┤           │
│  │ Gonderen Adi        │ Gonderen E-posta    │           │
│  │ [Machinity        ] │ [hello@machinity  ] │           │
│  ├─────────────────────┼─────────────────────┤           │
│  │ Gunluk Hedef        │ Gunluk Gonderim Cap│           │
│  │ [20              ] │ [20              ]  │           │
│  ├─────────────────────┴─────────────────────┤           │
│  │ Urun Aciklamasi                            │           │
│  │ [AI teammate that manages field-operation │           │
│  │  projects end-to-end by auto-capturing... ]│           │
│  └────────────────────────────────────────────┘           │
│                                                           │
│  [Profili Kaydet →]                                       │
└──────────────────────────────────────────────────────────┘
```

### 14.5 Adim 4: Ilk Calisma

```
┌──────────────────────────────────────────────────────────┐
│  ADIM 4: Ilk Prospecting Calismasi                        │
│                                                           │
│  Profiliniz hazir! Ilk calismayi baslatarak sistemi       │
│  test edin. Bu calisma ~2-4 dakika surebilir.             │
│                                                           │
│  [Ilk Calismayi Baslat]                                   │
│                                                           │
│  [Calisma suruyorsa: Ilerleme cubugu + asamalar]          │
│  ├── Sorgu plani olusturuluyor...        ✓                │
│  ├── Sirketler kesfediliyor...           ✓                │
│  ├── Profiller zenginlestiriliyor...     ◌                │
│  └── Leadler olusturuluyor...            ◌                │
│                                                           │
│  [Tamamlaninca: Sonuc tablosu]                            │
│  Bulunan: 20 | Hazir: 15 | Onay Bekleyen: 12             │
│                                                           │
│  [Panoya Git →]                                           │
└──────────────────────────────────────────────────────────┘
```

---

## 15. Ekran 2: Komuta Merkezi (Ana Pano)

### 15.1 Ust Istatistik Kartlari

```
┌──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
│ Toplam   │ Profil   │ Contact  │ Email    │ Bekleyen │ Teslim   │
│ Run      │ Sayisi   │ Ready    │ Only     │ Onay     │ Edilen   │
│          │          │ (gercek  │ (info@)  │          │          │
│   3      │   32     │ kisi)    │          │   31     │    0     │
│          │          │   10     │   20     │ [!]      │          │
│          │          │ [yesil]  │ [sari]   │ [kirmizi]│ [gri]   │
└──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
                                    ^^^
                        YENi: "Contact Ready" ve "Email Only" ayrimi
```

### 15.2 Calisma Gecmisi

```
┌──────────────────────────────────────────────────────────┐
│  Calisma Gecmisi                         [Yeni Run Al]   │
│                                                           │
│  Tarih          Durum      Kesif  Hazir  Onay   Puan Ort │
│  ─────────────  ─────────  ─────  ─────  ─────  ──────── │
│  26.03 13:01    tamamlandi   20     15     12    720/1000 │
│  26.03 12:52    tamamlandi   10      8      6    650/1000 │
│  03.03 16:51    tamamlandi    1      1      1    450/1000 │
│                                                           │
│  [Profilleri Goster]  [Sonuclari Karsilastir]            │
└──────────────────────────────────────────────────────────┘
```

### 15.3 Prospect Listesi (Ana Tablo)

```
┌──────────────────────────────────────────────────────────┐
│  Prospect Profilleri                  [Filtre▼] [Ara...] │
│                                                           │
│  ┌─────┬────────────────────┬────────┬────────┬────────┐ │
│  │Puan │ Sirket             │ Durum  │ Kisi   │ Kanal  │ │
│  ├─────┼────────────────────┼────────┼────────┼────────┤ │
│  │ 920 │ Alarko             │🟢ready │ I.Garih│ email  │ │
│  │     │ alarko.com.tr      │        │Chairman│+linked │ │
│  │     │ "Turk muteahhit"   │        │        │+tel    │ │
│  ├─────┼────────────────────┼────────┼────────┼────────┤ │
│  │ 780 │ Borusancat         │🟢ready │ O.Sahin│ email  │ │
│  │     │ borusancat.com     │        │ CEO    │        │ │
│  ├─────┼────────────────────┼────────┼────────┼────────┤ │
│  │ 520 │ Aksarayvinc        │🟡email │ -      │ email  │ │
│  │     │ aksarayvinc.net    │ only   │        │(info@) │ │
│  ├─────┼────────────────────┼────────┼────────┼────────┤ │
│  │ 320 │ Zimaszincir        │⚪co.   │ -      │ -      │ │
│  │     │ zimaszincir.com.tr │ only   │        │        │ │
│  └─────┴────────────────────┴────────┴────────┴────────┘ │
│                                                           │
│  Filtre: [Tumu▼] [Ready] [Email Only] [Identified] [Co.] │
│  Siralama: [Puan▼] [Tarih] [Sirket Adi]                 │
└──────────────────────────────────────────────────────────┘
```

---

## 16. Ekran 3: Prospect Profil Detay

```
┌──────────────────────────────────────────────────────────┐
│  ← Geri                              Puan: 920/1000      │
│                                                           │
│  ALARKO CONTRACTING GROUP                                 │
│  alarko.com.tr | İstanbul, Turkiye                        │
│  Durum: 🟢 Contact Ready | Arastirma: LLM %92 guven     │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │ OZET                                                 │ │
│  │ Alarko, Turkiye'nin en buyuk muteahhitlik            │ │
│  │ gruplarindan biri. TMB uyeligi ile dogrulanmis.      │ │
│  │ Cok sayida saha operasyonu, taserron yonetimi        │ │
│  │ ve proje koordinasyonu ihtiyaci. Machinity'nin       │ │
│  │ gorev takip ve gecikme yonetimi ozellikleri          │ │
│  │ dogrudan deger tasiyabilir.                          │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  BIRINCIL ILETISIM                                       │
│  ┌─────────────────────────────────────────────────────┐ │
│  │ Kisi:    Izzet Garih                                 │ │
│  │ Unvan:   Chairman                                    │ │
│  │ E-posta: info@alarko.com.tr [genel]                  │ │
│  │ Tel:     -                                           │ │
│  │ LinkedIn: -                                          │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  PUAN DAGILIMI                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │ Temel:     80/200  (TMB uye)                        │ │
│  │ Iletisim: 100/300  (kisi + genel email)             │ │
│  │ ICP:      240/300  (LLM %92 guven + sektor + site)  │ │
│  │ Sinyal:   100/200  (TMB + site icerik)              │ │
│  │ TOPLAM:   520/1000                                   │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ACI NOKTALARI (LLM uretimli)                           │
│  • Alarko'nun 50+ esanli sahasi var, taserron            │
│    koordinasyonu ve is emri takibi dagiik                 │
│  • Proje gecikme bildirimleri el ile, kayiplar           │
│    toplanti sonrasi kaybolabiliyor                        │
│  • Mobil saha ekipleri merkez ofisle kopuk                │
│                                                           │
│  TETIKLEYICILER                                          │
│  • TMB uyesi — saha operasyonlari odakli                 │
│  • Chairman seviyesinde erisilebilirlik                   │
│  • Web sitesinde "altyapi" ve "insaat" vurgusu           │
│                                                           │
│  OSINT KAYNAKLAR                                         │
│  • tmb.org.tr/en/members                                 │
│  • alarko.com.tr                                         │
│                                                           │
│  ERISIM STRATEJISI                                       │
│  Onerilen: E-posta → 7 gun → LinkedIn takip              │
│  Aci: TMB uyeligi uzerinden saha koordinasyon deger      │
│  onerisi ile giris                                        │
│                                                           │
│  [Mesaj Olustur] [Profili Duzenle] [Engelle]             │
└──────────────────────────────────────────────────────────┘
```

---

## 17. Ekran 4: Mesaj Onay Kuyrugu

```
┌──────────────────────────────────────────────────────────┐
│  Mesaj Onay Kuyrugu                                       │
│  Bekleyen: 12 | Bugun onaylanan: 5 | Kalan kota: 15     │
│                                                           │
│  [Toplu Islem: ✓ Sec] [Secilileri Onayla] [Secilileri Red│
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │ ☐ 920pt | Alarko | Izzet Garih | email              │ │
│  │                                                       │ │
│  │ Konu: Alarko saha koordinasyonunda Machinity          │ │
│  │                                                       │ │
│  │ Merhaba Izzet Bey,                                    │ │
│  │                                                       │ │
│  │ TMB uyesi Alarko'nun cok sayida esanli saha           │ │
│  │ projesinde taserron koordinasyonu ve gecikme           │ │
│  │ yonetimi ihtiyaci oldugunu gordum. Machinity          │ │
│  │ tam da bu noktada yardimci oluyor...                  │ │
│  │                                                       │ │
│  │ [Tam Mesaji Gor] [Duzenle] [Onayla ✓] [Reddet ✗]   │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐ │
│  │ ☐ 520pt | Aksarayvinc | - | email (info@)           │ │
│  │                                                       │ │
│  │ ⚠ Genel e-posta (info@aksarayvinc.net)              │ │
│  │ Onerilir: Onay vermeden once kisi bulmaya calisin    │ │
│  │                                                       │ │
│  │ [Tam Mesaji Gor] [Duzenle] [Onayla ✓] [Reddet ✗]   │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

---

## 18. Ekran 5: Teslimat ve Analitik

```
┌──────────────────────────────────────────────────────────┐
│  Teslimat Raporu                                          │
│                                                           │
│  ┌──────────┬──────────┬──────────┬──────────┐           │
│  │Gonderilen│ Acilan   │ Yanit    │ Bounce   │           │
│  │   28     │   12     │    3     │    2     │           │
│  │          │  %43     │  %11     │   %7     │           │
│  └──────────┴──────────┴──────────┴──────────┘           │
│                                                           │
│  TESLIMAT GECMISI                                         │
│  Tarih     Kanal   Alici              Durum    Yanit     │
│  ────────  ──────  ─────────────────  ───────  ──────    │
│  26.03     email   izzet@alarko.com   acildi    -        │
│  26.03     email   ali@abc.com        yanit     ✓ pozitif│
│  26.03     email   info@xyz.com       bounce    -        │
│  25.03     linked. /in/mehmet         goruldu   -        │
│                                                           │
│  TAKIP GEREKEN                                           │
│  • Alarko — 3 gundur yanit yok, LinkedIn takip onerisi  │
│  • Borusancat — e-posta acildi, 2. mesaj hazir          │
│                                                           │
│  HAFTALIK OZET                                           │
│  Bu hafta: 28 gonderim, 3 yanit (%11), 1 demo takvimi   │
│  Gecen hafta: 15 gonderim, 1 yanit (%7), 0 demo         │
└──────────────────────────────────────────────────────────┘
```

---

## 19. Ekran 6: Ayarlar ve Profil Yonetimi

```
┌──────────────────────────────────────────────────────────┐
│  Ayarlar                                                  │
│                                                           │
│  URUN PROFILI                                            │
│  ┌──────────────────────────────────────────────────┐    │
│  │ Urun Adi:        [Machinity                    ] │    │
│  │ Urun Aciklamasi: [AI teammate that manages...  ] │    │
│  │ Hedef Sektor:    [Field service and on-site... ] │    │
│  │ Hedef Cografya:  [TR                           ] │    │
│  └──────────────────────────────────────────────────┘    │
│                                                           │
│  GONDEREN BILGILERI                                      │
│  ┌──────────────────────────────────────────────────┐    │
│  │ Gonderen Adi:    [Machinity                    ] │    │
│  │ E-posta:         [hello@machinity.com          ] │    │
│  │ LinkedIn:        [linkedin.com/company/machinit] │    │
│  └──────────────────────────────────────────────────┘    │
│                                                           │
│  CALISMA AYARLARI                                        │
│  ┌──────────────────────────────────────────────────┐    │
│  │ Gunluk Hedef:        [20        ] profil        │    │
│  │ Gunluk Gonderim Cap: [20        ] mesaj         │    │
│  │ Zamanlama Saati:     [09:00     ] yerel saat    │    │
│  │ Unvan Politikasi:    [CEO sonra Founder ▼]      │    │
│  └──────────────────────────────────────────────────┘    │
│                                                           │
│  BAGLANTI                                                │
│  Codex OAuth: [● Bagli] [Cikis Yap]                     │
│  SMTP:        [● Ayarli] [Test Gonder]                   │
│                                                           │
│  ENGELLENEN DOMAINLER                                    │
│  gmail.com, outlook.com, yahoo.com (otomatik)            │
│  + Kullanici eklenen: [________________] [Ekle]          │
│  • example.com [Kaldir]                                   │
│                                                           │
│  [Kaydet]                                                │
└──────────────────────────────────────────────────────────┘
```

---

# KISIM D — TEKNIK ALTYAPI

## 20. Veritabani Semasi

### 20.1 Mevcut Tablolar (8 adet, sales.db)

```sql
-- 1. Tekil profil yapilandirmasi
CREATE TABLE sales_profile (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- 2. Calisma gecmisi
CREATE TABLE sales_runs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,        -- running | completed | failed
    started_at TEXT NOT NULL,
    completed_at TEXT,
    discovered INTEGER DEFAULT 0,
    inserted INTEGER DEFAULT 0,
    approvals_queued INTEGER DEFAULT 0,
    error TEXT
);

-- 3. Bulunan leadler
CREATE TABLE leads (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    company TEXT, website TEXT, company_domain TEXT,
    contact_name TEXT, contact_title TEXT,
    linkedin_url TEXT, email TEXT, phone TEXT,
    reasons_json TEXT,
    email_subject TEXT, email_body TEXT, linkedin_message TEXT,
    score INTEGER, status TEXT, created_at TEXT,
    UNIQUE(company_domain, contact_name, contact_title)
);

-- 4. Onay kuyrugu
CREATE TABLE approvals (
    id TEXT PRIMARY KEY,
    lead_id TEXT, channel TEXT, payload_json TEXT,
    status TEXT,                 -- pending | approved | rejected
    created_at TEXT, decided_at TEXT
);
CREATE INDEX idx_approvals_status ON approvals(status, created_at DESC);

-- 5. Teslimat kaydi
CREATE TABLE deliveries (
    id TEXT PRIMARY KEY,
    approval_id TEXT, channel TEXT, recipient TEXT,
    status TEXT,                 -- sent | failed | bounced | opened | replied
    error TEXT, sent_at TEXT
);
CREATE INDEX idx_deliveries_sent ON deliveries(sent_at DESC);

-- 6. Onboarding brief
CREATE TABLE sales_onboarding (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    brief_text TEXT, updated_at TEXT
);

-- 7. Domain tekilsizlestirme
CREATE TABLE discovered_domains (
    domain TEXT PRIMARY KEY,
    first_seen_run_id TEXT, first_seen_at TEXT
);

-- 8. Prospect profilleri
CREATE TABLE prospect_profiles (
    company_domain TEXT PRIMARY KEY,
    run_id TEXT, json TEXT,
    created_at TEXT, updated_at TEXT
);
CREATE INDEX idx_pp_run ON prospect_profiles(run_id, updated_at DESC);
CREATE INDEX idx_pp_updated ON prospect_profiles(updated_at DESC);
```

### 20.2 Onerilen Yeni Tablolar

```sql
-- 9. Teslimat takip olaylari (YENi)
CREATE TABLE delivery_events (
    id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    event_type TEXT NOT NULL,    -- opened | clicked | replied | bounced
    event_at TEXT NOT NULL,
    metadata_json TEXT
);

-- 10. Engellenen domainler (YENi)
CREATE TABLE blocked_domains (
    domain TEXT PRIMARY KEY,
    reason TEXT,
    blocked_at TEXT
);

-- 11. A/B test varyantlari (YENi)
CREATE TABLE message_variants (
    id TEXT PRIMARY KEY,
    lead_id TEXT NOT NULL,
    variant TEXT NOT NULL,       -- A | B
    subject TEXT, body TEXT,
    selected INTEGER DEFAULT 0
);
```

---

## 21. API Endpoint Haritasi

### 21.1 Mevcut Endpointler

| Yontem | Yol | Islem |
|--------|-----|-------|
| GET | `/api/sales/profile` | Profili getir |
| PUT | `/api/sales/profile` | Profili guncelle |
| POST | `/api/sales/profile/autofill` | Brieften otomatik doldur |
| GET | `/api/sales/onboarding/status` | Onboarding durumu |
| POST | `/api/sales/onboarding/brief` | Brief kaydet |
| POST | `/api/sales/run` | Calisma baslat |
| GET | `/api/sales/runs?limit=20` | Calisma gecmisi |
| GET | `/api/sales/prospects?limit=200` | Prospect profilleri |
| GET | `/api/sales/prospects?run_id=X` | Run bazli profiller |
| GET | `/api/sales/leads?limit=200` | Leadler |
| GET | `/api/sales/approvals?limit=200` | Onay kuyrugu |
| POST | `/api/sales/approvals/{id}/approve` | Onayla ve gonder |
| POST | `/api/sales/approvals/{id}/reject` | Reddet |
| GET | `/api/sales/deliveries?limit=200` | Teslimatlar |

### 21.2 Onerilen Yeni Endpointler

| Yontem | Yol | Islem |
|--------|-----|-------|
| GET | `/api/sales/analytics` | Haftalik/aylik analitik |
| GET | `/api/sales/analytics/funnel` | Huni metrikleri |
| POST | `/api/sales/approvals/bulk-approve` | Toplu onay |
| POST | `/api/sales/approvals/bulk-reject` | Toplu red |
| PATCH | `/api/sales/approvals/{id}/edit` | Mesaj duzenleme |
| POST | `/api/sales/prospects/{domain}/block` | Domain engelle |
| DELETE | `/api/sales/prospects/{domain}/block` | Engel kaldir |
| GET | `/api/sales/deliveries/{id}/events` | Teslimat olaylari |
| POST | `/api/sales/followup/{delivery_id}` | Takip mesaji olustur |
| GET | `/api/sales/blocked-domains` | Engelli domain listesi |

---

## 22. LLM Entegrasyonu ve Prompt Muhendisligi

### 22.1 Kullanilan Modeller

| Islem | Model | Max Token | Sicaklik | Zaman Asimi |
|-------|-------|-----------|----------|-------------|
| Sorgu plani | gpt-5.3-codex | 900 | 0.0 | 4sn |
| Sirket uretimi | gpt-5.3-codex | 2400 | 0.2 | 10sn |
| Ilgilik dogrulama | gpt-5.3-codex | 1400 | 0.0 | 6sn |
| Profil zenginlestirme | gpt-5.3-codex | 2000 | 0.2 | 18sn |
| Mesaj uretimi (YENi) | gpt-5.3-codex | 1200 | 0.4 | 8sn |
| Brief'ten profil | gpt-5.3-codex | 1500 | 0.1 | 6sn |

### 22.2 Prompt Sablonlari

**Sirket Kesfii:**
```
Sen B2B pazar arastirmacisisin. Asagidaki urun ve ICP icin
{max_companies} adet gercek sirket listele.

Urun: {product_description}
Sektor: {target_industry}
Cografya: {target_geo}
Sirket buyuklugu: 20-5000 calisan (KOBi/orta pazar)

KURALLAR:
- Gercek, var olan sirketler (hayali degil)
- Her sirket icin: name, domain, reason
- linkedin.com, wikipedia.org gibi genel siteleri dahil etme
- Daha once bulunanlar: {existing_domains}

JSON dizisi olarak dondur.
```

**Ilgilik Dogrulamasi:**
```
Sen Turk B2B pazar analistisin. Her adayi ICP'ye karsi degerlendir.

ICP: {target_industry}
Cografya: {target_geo}

Adaylar: {candidates_json}

Her aday icin: {domain, relevant: bool, confidence: 0.0-1.0, reason: string}
JSON dizisi olarak dondur.
```

---

## 23. Zamanlama ve Otomasyon

### 23.1 Gunluk Otomatik Calisma

```
spawn_sales_scheduler() — Arka plan gorevi

Her 5 dakikada bir:
  1. Profil yukle
  2. Saat kontrolu: now.hour == schedule_hour_local && now.minute <= 10
  3. Bugun zaten calistirildi mi? → already_ran_today() kontrolu
  4. Tum kosullar saglanirsa: run_generation() baslat
  5. Zaman asimi: 120 saniye (zamanlayici icin)

Varsayilan: Her gun saat 09:00-09:10 arasi tetiklenir.
```

### 23.2 Onay Kota Yonetimi

```
Gunluk gonderim cap'i:
  - approve_and_send() icinde kontrol edilir
  - deliveries_today() ile bugunki teslimat sayisi sorgulanir
  - sent_today >= daily_send_cap ise reddedilir
  - Saat dilimi duyarli (timezone_mode)
```

---

## 24. Guvenlik ve Uyumluluk

### 24.1 Veri Guvenligi
- OAuth tokenleri memory-only (diske yazilmaz — sadece runtime)
- SMTP kimlik bilgileri cevresel degiskenlerden
- API anahtari Bearer token ile korunmus
- Loopback-only mod (api_key yoksa sadece localhost)

### 24.2 E-posta Uyumlulugu
- CAN-SPAM / KVKK uyumu icin:
  - Gonderen bilgisi acik
  - Abonelikten cikmaya izin ver (footer'da link)
  - Fiziksel adres bilgisi
- Gunluk gonderim cap'i ile hiz sinirlamasi
- Bounce takibi ile liste hijyeni

### 24.3 GDPR/KVKK
- Sadece kamusal veriler islenir (web siteleri, dizinler)
- Kisi verileri onay ile islenir (insan-dongusu)
- Silme hakki: prospect_profiles tablosundan kaldirilabilir
- Veri minimizasyonu: Sadece is amaciyla gerekli alanlar

---

# KISIM E — METRIKLER VE YOL HARITASI

## 25. Basari Metrikleri ve KPI'lar

### 25.1 Kesif Metrikleri

| Metrik | Mevcut | Hedef | Olcum |
|--------|--------|-------|-------|
| Run basi bulunan sirket | 20-30 | 30-50 | discovered/run |
| Gercek kisi adi bulma | %38 | %70+ | name_found/total |
| LinkedIn profil bulma | %0 | %30+ | linkedin_found/total |
| Telefon bulma | %0 | %40+ | phone_found/total |
| Kisisel e-posta bulma | %0 | %20+ | personal_email/total |
| Gecersiz domain (gmail vb.) | %3 | %0 | invalid/total |

### 25.2 Erisim Metrikleri

| Metrik | Mevcut | Hedef | Olcum |
|--------|--------|-------|-------|
| E-posta acilma orani | Olculmuyor | %25+ | opened/sent |
| E-posta yanit orani | Olculmuyor | %5+ | replied/sent |
| LinkedIn gorulme | Olculmuyor | %40+ | seen/sent |
| Bounce orani | Olculmuyor | <%5 | bounced/sent |
| Demo/toplanti | Olculmuyor | %2+ | meeting/sent |

### 25.3 Kalite Metrikleri

| Metrik | Mevcut | Hedef | Olcum |
|--------|--------|-------|-------|
| Onay orani | Olculmuyor | %80+ | approved/total_approvals |
| Ortalama kalite puani | 100 (hepsi ayni) | 600+ (dagilim) | avg(fit_score) |
| info@ e-posta orani | %80+ | %0 | info_email/total |
| Sablon tekrar orani | %100 | %0 (LLM) | template/total |

---

---

===============================================================
# PARCA 4 — UZMAN DEGERLENDIRMESI VE GERCEK IMPLEMENTASYON PLANI
===============================================================

> Asagidaki bolum, Parca 1-3'teki analiz ve hedef mimarinin bir uzman
> LLM (ChatGPT Pro) tarafindan degerlendirilmesi sonucu ortaya cikan
> kritik tespitleri ve bunlara dayanan GUNCELLENMIS implementasyon
> planini icerir.

---

## 27. Uzman Degerlendirmesi: Mevcut Skor Karti

| Katman | Mevcut Puan | Aciklama |
|--------|-------------|----------|
| Kesif (Discovery) | 7/10 | TR dizin paketleri guclu, LLM kesfii calisiyor |
| Zenginlestirme (Research) | 5/10 | Site + arama OSINT var ama LinkedIn %0, telefon kayip |
| Aktivasyon (Activation) | 3/10 | Sablon mesajlar, tek skor, onay kuyrugu ilkel |
| Ogrenme (Learning) | 1/10 | Bounce/yanit/outcome takibi neredeyse yok |

**Temel Tespit:** Sistem "kac aday buldum?" sorusunu optimize ediyor.
Oysa para getiren makine "en guvenilir sekilde en cok olumlu cevabi
ureten" makinedir.

---

## 28. Kabul Edilen Mimari Elestiriler

### 28.1 Kuzey Yildizi Yanlis

**Mevcut metrikler:** discovered, inserted, approvals_queued (uretim metrikleri)
**Olmasi gereken:** positive_reply_rate, meeting_rate, opportunity_rate,
spam_risk, dogru_kisi_orani (is sonucu metrikleri)

### 28.2 Discovery ile Activation Yapisik

`run_generation()` tek bir akista kesif + arastirma + lead uretimi +
onay kuyrugu yapiyor. Bu yanlis.

**Dogru ayrim:**
- **Discovery Reservoir** — surekli hesap evreni doldurur
- **Research Queue** — eksik kimlikleri tamamlar
- **Activation Queue** — sadece bugun gonderilecek en iyi firsatlari secer

`daily_target` SADECE gonderimi sinirlamali, kesfii degil.

### 28.3 Lead-Merkezli Degil, Account-Merkezli Olmali

Bir domain = bir prospect varsayimi B2B icin fazla sig.
Gercekte: bir hesabin birden fazla domaini, holdinglerin alt markalari,
ayni sirketin farkli ulkelerde farkli alan adlari olabilir.

Ihtiyac: **Lead tablosu degil, Account Graph.**
Node'lar: account, domain, contact, signal, message, reply
Edge'lar: works_at, mentioned_in, derived_from, replied_to, referred_to

### 28.4 LLM Pipeline'da Cok Erken

LLM ile sirket uretimi ve relevance validation mantikli gorunuyor ama
model "dusunur gibi" gorunurken dusuk guvenli veri enjekte edebilir.

**Dogru yaklasim:**
- LLM kesfitte opsiyonel, dusuk guvenli yardimci
- LLM sentezde (ozet, mesaj, pain point) ana arac
- Deterministik katmanlar dogrulama, dedup, idempotency, policy, scheduling

### 28.5 Tek Skor Yetersiz — 5 Eksenli Puanlama

Tek skor (0-100 veya 0-1000) sunlari birbriine karistiriyor:

| Eksen | Soru | Ornek |
|-------|------|-------|
| **FitScore** | ICP'ye uyuyor mu? | Sektor eslesmesi, buyukluk, cografya |
| **IntentScore** | Simdi dogru zaman mi? | Yeni tesis, ihale, buyume sinyali |
| **ReachabilityScore** | Gercek insana ulasabiliyor muyum? | Kisisel email, LinkedIn, telefon |
| **DeliverabilityRisk** | Gondermek guvenli mi? | Bounce riski, domain itibari |
| **ComplianceRisk** | Yasal risk var mi? | KVKK, opt-out, suppression |

Sonra bir **Send Gate** koyulur:
Yuksek oncelikli hesap bile gonderilebilir olmak ZORUNDA degildir.

### 28.6 Alan Bazli Guven Skoru

`research_confidence` profil seviyesinde cok kaba. Gercek ihtiyac:

```
company_name_confidence: 0.95
domain_confidence: 0.99
contact_name_confidence: 0.40   // "Baskan'in Mesaji" gibi seyler
title_confidence: 0.60
email_confidence: 0.30          // info@ = dusuk
linkedin_confidence: 0.00
phone_confidence: 0.80          // dizinden gelmis, dogrulanmamis
```

Operator "profil %87 guvenli" degil, "isim supeli, email zayif" bilgisini ister.

### 28.7 Domain Bazli Dedup Ilkel

`company_domain` primary key + `discovered_domains` tekillestime hizli ama kirilgan.

**Dogru model:**
- canonical_account (ana hesap)
- account_alias (alternatif isimler)
- domain (birden fazla olabilir)
- location
- legal_entity_name
- brand_name

Dedup: domain + ad benzerligi + site kimligi + iletisim sinyali + kaynak dogrulama

### 28.8 LinkedIn Stratejik Hata

LinkedIn Help, ucuncu taraf yazilimlarla scrape/automation kullanimina
izin vermiyor. Resmi Marketing API de member data'nin sales/lead creation
icin kullanilmasini ve mass messaging'i yasakliyor.

**LinkedIn bu mimaride:** otomatik DM kanali degil,
**icerik + manuel asist + first-party engagement** kanali olmali.

### 28.9 Bounce/Reply Intelligence Faz 5'te Degil, Faz 1'de Olmali

Sonuc verisini toplamadan skoru da mesaji da kalibre edemezsin.
Bu "ileri faz" degil, temel altyapi.

### 28.10 Frontend/Backend Schema Drift

Rust: `target_geo = "US"`, JS: `target_geo = "TR"` — bu kucuk bug degil,
contract-first gelistirme eksikliginin isareti.

---

## 29. Hedef Mimari: Revenue Graph + Activation Engine + Learning Loop

Tek boru hatti yerine **3 buyuk motor**:

```
                     REVENUE GRAPH
                     (kim hedef, neden simdi, hangi kanitla)
                          |
                     ACTIVATION ENGINE
                     (kime, hangi sirayla, hangi kanalla, hangi mesaj)
                          |
                     LEARNING LOOP
                     (ne ise yaradi, ne yaramadi, neyi kapat, neyi buyut)
```

### 29.1 Tam Akis

```
[First-party signals]   [External source packs]   [CRM/history]
        \                     |                        /
         \                    |                       /
          ---> [Evidence Ingestion + Source Health] ---->
                    |
                    v
             [Account / Contact Graph]
                    |
                    v
      [Fit + Intent + Reachability + Risk Engine]
                    |
         +----------+-----------+
         |                      |
         v                      v
 [Research Queue]        [Activation Queue]
         |                      |
         v                      v
 [Evidence-bound Dossier] [Sequence Planner]
         |                      |
         +----------+-----------+
                    v
        [Message Generation + QA + Policy]
                    |
                    v
       [Sendability / Compliance Guardrail]
                    |
         +----------+-----------+
         |                      |
         v                      v
   [Email Execution]    [LinkedIn Manual Assist / Call Task]
         |
         v
 [Inbox / Bounce / Reply / Unsubscribe Ingestion]
         |
         v
 [Suppression + Experimentation + Score Calibration]
```

### 29.2 ICP Control Plane

Tek `sales_profile` yerine:
- ICP tanimlari (birden fazla)
- Alt segmentler
- Negatif ICP kurallari
- Persona haritasi
- Mesaj stratejileri
- Sender policy
- Scoring version + prompt version

Profil bir form degil, **strateji nesnesi**.

### 29.3 Source Pack Katmani

TR field-ops pack'i genellestir:
- `TR_FieldOps` (TMB, ASMUD, MIB, IMDER, ISDER, THBB, Platformder)
- `TR_Energy` (EUD)
- `EU_Facility` (gelecek)
- `US_FieldService` (gelecek)

Her kaynak adapter'in contract'i:
- raw artifact
- extraction result
- source confidence + freshness
- parse status + parser health
- legal notes

### 29.4 Account Graph — Graphiti Acik Kaynak Temporal Knowledge Graph

> **Graphiti** (Apache 2.0, github.com/getzep/graphiti, v0.28.2) acik kaynak
> kutuphanesi dogrudan kullanilir. arXiv:2501.13956, Rasmussen et al. 2025.
> Dis bulut servisi YOK — Graphiti'nin **Kuzu embedded driver'i** ile
> tamamen yerel (offline) calisir. Neo4j gerekmez.

Account graph icin Graphiti'nin zamansal-duyarli (temporally-aware)
bilgi grafi motorunu dogrudan kullaniyoruz. Python kutuphanesi olarak
OpenFang'in `process_manager` veya ayri bir sidecar sureci uzerinden
calistirilir.

**Neden Graphiti (dogrudan kutuphane)?**
- Mevcut OpenFang `KnowledgeStore` statik entity-relation modeli kullaniyor.
  Prospecting'de ise veriler surekli degisiyor: kisi unvan degistirir, sirket
  el degistirir, e-posta gecersiz olur, yeni sinyal gelir.
- Graphiti bi-temporal model ile hem "gercekte ne zaman oldu" (T timeline)
  hem "sisteme ne zaman girdi" (T' timeline) takip eder.
- 3 katmanli hiyerarsi (Episode → Semantic Entity → Community) prospecting
  pipeline'inin kesfetten analize, analizden aksiyona akisina birebir uyar.
- **Kuzu embedded driver** (`KuzuDriver(db='~/.openfang/data/prospect_graph.kuzu')`)
  ile tamamen dosya-bazli, sunucusuz calisir — SQLite gibi ama graf icin.
- Apache 2.0 lisansli, ticari kullanima uygun.

**Teknik profil:**
```
Kutuphane:  graphiti-core v0.28.2
Lisans:     Apache 2.0
Dil:        Python >=3.10
Graf DB:    Kuzu embedded (sunucusuz, dosya-bazli) ← TERCIH
            + Neo4j, FalkorDB, Neptune (opsiyonel)
LLM:        OpenAI (varsayilan) + Anthropic, Groq, Gemini (opsiyonel)
Embedder:   OpenAI (varsayilan) + Voyage, sentence-transformers (opsiyonel)
Reranker:   OpenAI, BGE, Gemini
Bagimllik:  pydantic, neo4j (sdk, kuzu icin gerekmez), openai, numpy
```

**OpenFang entegrasyonu:**
```
OpenFang (Rust) ←→ Graphiti (Python) baglantisi:
  Secenek A: OpenFang python_runtime ile subprocess olarak calistir
  Secenek B: OpenFang process_manager ile kalici Python sureci
  Secenek C: Graphiti MCP server modu (graphiti/mcp_server/) ile MCP uzerinden

  Tercih: Secenek C (MCP) — OpenFang zaten MCP destegi olan bir platform,
  Graphiti'nin kendi MCP server'i var, sifir ek kod ile entegrasyon.
```

#### 29.4.1 Kuzu Embedded DB Semasi (Graphiti otomatik olusturur)

```sql
-- Graphiti'nin KuzuDriver'i baslatildiginda otomatik olusturulan sema
-- Kaynak: graphiti_core/driver/kuzu_driver.py SCHEMA_QUERIES

CREATE NODE TABLE IF NOT EXISTS Episodic (
    uuid STRING PRIMARY KEY,
    name STRING,
    group_id STRING,            -- prospect run ID ile grupla
    created_at TIMESTAMP,
    source STRING,              -- "text" | "json" | "message"
    source_description STRING,  -- "TMB members scrape" vb.
    content STRING,             -- ham episode icerigi
    valid_at TIMESTAMP,
    entity_edges STRING[]
);

CREATE NODE TABLE IF NOT EXISTS Entity (
    uuid STRING PRIMARY KEY,
    name STRING,                -- "Alarko", "Izzet Garih"
    group_id STRING,
    labels STRING[],            -- ["Account"], ["Contact"] vb.
    created_at TIMESTAMP,
    name_embedding FLOAT[],     -- 1024-dim vektor
    summary STRING,             -- entity ozeti
    attributes STRING           -- JSON: {sector, geo, title, ...}
);

CREATE NODE TABLE IF NOT EXISTS Community (
    uuid STRING PRIMARY KEY,
    name STRING,                -- "TR Insaat Muteahhitleri"
    group_id STRING,
    created_at TIMESTAMP,
    name_embedding FLOAT[],
    summary STRING              -- topluluk ozeti
);

CREATE NODE TABLE IF NOT EXISTS RelatesToNode_ (
    uuid STRING PRIMARY KEY,
    group_id STRING,
    created_at TIMESTAMP,
    name STRING,                -- edge/fact kisa adi
    fact STRING,                -- "Alarko employs Izzet Garih as Chairman"
    fact_embedding FLOAT[],     -- fact vektoru
    episodes STRING[],          -- kaynak episode UUID'leri
    expired_at TIMESTAMP,       -- t'_expired (sistemde gecersiz kilindi)
    valid_at TIMESTAMP,         -- t_valid (gercekte basladigi tarih)
    invalid_at TIMESTAMP,       -- t_invalid (gercekte bittigi tarih)
    attributes STRING           -- JSON: {confidence, channel_type, ...}
);

-- Iliskiler
CREATE REL TABLE IF NOT EXISTS RELATES_TO(
    FROM Entity TO RelatesToNode_,
    FROM RelatesToNode_ TO Entity
);
CREATE REL TABLE IF NOT EXISTS MENTIONS(
    FROM Episodic TO Entity      -- episode hangi entity'leri cikartti
);
CREATE REL TABLE IF NOT EXISTS HAS_MEMBER(
    FROM Community TO Entity,    -- community hangi entity'leri icerir
    FROM Community TO Community
);
CREATE REL TABLE IF NOT EXISTS HAS_EPISODE(
    FROM Saga TO Episodic        -- saga (run grubu) hangi episode'lari icerir
);
CREATE REL TABLE IF NOT EXISTS NEXT_EPISODE(
    FROM Episodic TO Episodic    -- kronolojik siralama
);
```

#### 29.4.1b Uc Katmanli Graf Yapisi

```
┌─────────────────────────────────────────────────────┐
│           COMMUNITY SUBGRAPH (Gc)                    │
│  Guclu bagli entity kümelerinin ozet temsilcileri    │
│  Ornek: "TR Insaat Muteahhitleri", "Makine Sanayi"  │
│  Label propagation ile otomatik kumeleme             │
│  Community ozeti: map-reduce ile ozetleme            │
│  Yeni entity eklendiginde dinamik guncelleme         │
└──────────────────────┬──────────────────────────────┘
                       │ community_member
┌──────────────────────▼──────────────────────────────┐
│         SEMANTIC ENTITY SUBGRAPH (Gs)                │
│  Episode'lardan cikarilan varliklar ve iliskiler     │
│                                                      │
│  ENTITY DUGÜMLERI (Ns):                             │
│    Account — canonical_id, display_name, legal_name, │
│              employee_estimate, sector, geo          │
│    Contact — name, title, department, seniority      │
│    Domain — domain, verified, is_primary             │
│    Product — name, category                          │
│    Event — type(ihale|tesis|sertifika), date         │
│                                                      │
│  SEMANTIC KENARLAR (Es) — zamansal fact'ler:        │
│    Account --EMPLOYS--> Contact                      │
│      fact: "Alarko employs Izzet Garih as Chairman" │
│      valid_at: 2020-01-01, invalid_at: null         │
│    Account --HAS_DOMAIN--> Domain                    │
│    Contact --REACHABLE_VIA--> ContactMethod          │
│      fact: "Izzet reachable via info@alarko.com.tr" │
│      confidence: 0.3 (generic email)                 │
│    Account --OBSERVED_SIGNAL--> Signal               │
│    Account --USES_PRODUCT--> Product (tech-stack)    │
│    Account --PARTICIPATED_IN--> Event                │
│    Touch --SENT_TO--> Contact                        │
│    Reply --IN_RESPONSE_TO--> Touch                   │
│                                                      │
│  Her kenar (fact) sunlari tasir:                     │
│    t_valid: gercekligin basladigi tarih              │
│    t_invalid: gercekligin bittigi tarih (null=hala)  │
│    t'_created: sisteme giris tarihi                  │
│    t'_expired: sistemde gecersiz kilinma tarihi      │
│    source_episode_id: hangi episode'dan geldi        │
│    confidence: 0.0-1.0                               │
└──────────────────────┬──────────────────────────────┘
                       │ extracted_from
┌──────────────────────▼──────────────────────────────┐
│           EPISODE SUBGRAPH (Ge)                      │
│  Ham girdi verileri — kayipsiz (non-lossy) depo     │
│                                                      │
│  Episode turleri:                                    │
│    message: Konusma mesajlari, e-posta yanitlari    │
│    text: Site HTML icerigi, dizin listesi metni      │
│    json: Yapilandirilmis API ciktilari, CRM verisi  │
│                                                      │
│  Her episode sunlari tasir:                          │
│    content: ham metin/JSON                           │
│    actor: kim uretti (scraper, LLM, kullanici)       │
│    t_ref: referans zaman damgasi                     │
│    source: kaynak (TMB, web_search, site_html, vb.)  │
│                                                      │
│  Episode → Entity baglantilari cift yonlu:           │
│    Episode'dan entity'ye: "bu episode sunu cikartti" │
│    Entity'den episode'a: "bu entity su kaynaktan"    │
└─────────────────────────────────────────────────────┘
```

#### 29.4.2 Zamansal Yonetim (Temporal Management)

Graphiti'nin en kritik farki: **edge invalidation**. Yeni bilgi geldiginde
eski bilgiyi silmez, gecersiz kilar (invalidate). Ornek:

```
Zaman 1: "Ali Vural is CEO of ABC Ltd"
  → Edge: Ali --WORKS_AT--> ABC, title=CEO
    valid_at: 2023-01, invalid_at: null

Zaman 2: "Ali Vural left ABC Ltd, joined XYZ Inc as CTO"
  → Eski edge guncellenir: invalid_at = 2025-03
  → Yeni edge: Ali --WORKS_AT--> XYZ, title=CTO
    valid_at: 2025-03, invalid_at: null

Sonuc: Her iki iliski de grafta kalir.
Sorguda "Ali simdi nerede?" → XYZ (invalid_at = null)
Sorguda "Ali 2024'te neredeydi?" → ABC (tarih araligina gore)
```

Bu, prospecting icin KRITIK: Insanlar is degistirir, sirketler el degistirir,
e-postalar gecersiz olur. Geleneksel statik graf bunu yonetemez.

#### 29.4.3 Episode Turleri ve Kaynak Esleme

Prospecting pipeline'daki her veri kaynagi bir episode turune eslesir:

| Kaynak | Episode Turu | Actor | Ornek Icerik |
|--------|-------------|-------|--------------|
| TMB dizin taramasi | text | scraper:tmb | "ALARKO CONTRACTING GROUP, Chairman: Izzet Garih, web: alarko.com.tr" |
| ASMUD uye listesi | text | scraper:asmud | "FERNAS INSAAT A.S., tel: ..., email: ankaramerkez@fernas.com.tr" |
| Site HTML | text | scraper:site | "Hakkimizda... 50 yillik deneyim... altyapi ve insaat" |
| Web arama sonucu | json | search:brave | {"title": "...", "url": "...", "snippet": "..."} |
| LLM sirket uretimi | json | llm:codex | {"company": "...", "domain": "...", "reason": "..."} |
| LLM profil zenginlestirme | json | llm:codex | {"summary": "...", "pain_points": [...]} |
| E-posta yaniti | message | reply:email | "Tesekkurler, su an icin ihtiyacimiz yok" |
| Bounce bildirimi | json | system:smtp | {"type": "hard_bounce", "email": "..."} |
| Kullanici duzenleme | message | user:operator | "Bu kisi artik CEO degil, CFO olmus" |

Her episode Graphiti'ye `add_episode()` ile gonderilir. Graphiti sirasiyla:
1. **Entity extraction** — LLM ile varlik cikarma (son 4 mesaj baglam)
   + reflexion teknigi ile hallucination azaltma
2. **Entity resolution** — embedding cosine similarity + fulltext arama
   ile mevcut entity esleme, LLM ile dogrulama
3. **Fact extraction** — entity ciftleri arasindaki iliskileri cikarma,
   her fact icin anahtar yuklem (predicate) olusturma
4. **Fact resolution** — ayni entity cifti arasindaki mevcut fact'lerle
   karsilastirma, duplicate tespiti
5. **Temporal extraction** — t_ref referans zamani ile mutlak/goreli
   tarih cikarma, celisen fact'lerin invalidation'i
6. **Community detection** — label propagation ile kumeleme,
   yeni entity eklendiginde dinamik guncelleme

#### 29.4.4 Arama ve Geri Getirme

Graphiti'nin dahili arama sistemi 3 yontemi birlestirerek kullanir:

```python
# Graphiti search API — tek cagirim, 3'lu hibrit arama
results = await graphiti.search(
    query="insaat sektoru CEO'lari Turkiye",
    group_ids=["prospect_run_2026_03_26"],
    num_results=20,
    search_config=SearchConfig(...)  # opsiyonel: ince ayar
)
# results.edges → fact'ler (valid_at/invalid_at ile)
# results.nodes → entity'ler (name + summary)
# results.communities → topluluk ozetleri

# Dahili akis:
# f(sorgu) = constructor( reranker( search(sorgu) ) )

# search(sorgu) = {
#   cosine_similarity: Entity name_embedding + fact_embedding uzerinden
#   bm25_fulltext: Entity name + fact metni uzerinden (Kuzu FTS)
#   breadth_first: Bulunan entity'lerin n-hop komsulugu
# }

# reranker:
#   RRF (Reciprocal Rank Fusion) — 3 arama sonucunu birlestirme
#   + episode-mentions — sik bahsedilen = daha erisilebilir
#   + opsiyonel cross-encoder — OpenAI/BGE/Gemini reranker

# constructor: FACTS + ENTITIES + COMMUNITIES → context string
```

Bu 3'lu arama, prospecting'in farkli ihtiyaclarini karsilar:
- **Semantik arama**: "insaat sektoru CEO'lari" → anlam bazli esleme
- **Tam metin arama**: "alarko.com.tr" → kesin esleme
- **Graf yurume**: Alarko'nun tum contact'lari, domain'leri, sinyalleri → baglam

#### 29.4.5 Prospecting Ontolojisi

Ontoloji tanimi, Graphiti'nin entity ve edge extraction sirasinda hangi
tipleri arayacagini belirler. Graphiti 29.4.1'deki Kuzu semasini otomatik
olusturur. Biz ek olarak LLM extraction prompt'larina ontoloji bilgisini
enjekte ediyoruz:

```python
# Prospecting ontoloji tanimi (Graphiti kullanimi)
# Graphiti'nin Kuzu driver'i su semavi otomatik olusturur:
#   Episodic (uuid, name, group_id, content, valid_at, ...)
#   Entity (uuid, name, labels[], summary, attributes, name_embedding[])
#   Community (uuid, name, summary, name_embedding[])
#   RelatesToNode_ (uuid, fact, fact_embedding[], valid_at, invalid_at, ...)
#
# Biz entity extraction prompt'una su ontoloji bilgisini ekliyoruz:

PROSPECTING_ENTITY_TYPES = """
Entity types to extract:
- Account: B2B target company / organization (display_name, legal_name, sector, geo, employee_estimate)
- Contact: Person at a target company, potential buyer (full_name, title, seniority, department)
- Domain: Web domain owned by an account (domain, is_primary, verified)
- ContactMethod: Communication channel (channel_type: email|phone|linkedin, value, confidence)
- Signal: Observable business signal (signal_type, text, source, observed_at)
- Event: Business event like tender, new facility, certification (event_type, date)
"""

PROSPECTING_EDGE_TYPES = """
Relationship types to extract:
- EMPLOYS: Account employs a Contact (with title attribute)
- HAS_DOMAIN: Account owns a Domain
- REACHABLE_VIA: Contact reachable through a ContactMethod
- OBSERVED_SIGNAL: Signal observed for an Account
- USES_PRODUCT: Account uses a Product (tech-stack)
- PARTICIPATED_IN: Account participated in an Event
"""

# Bu ontoloji Graphiti'nin entity/fact extraction prompt'larina
# ek context olarak eklenir (Graphiti custom prompt destegi var)
```

#### 29.4.6 Gercek Zamanli Guncelleme

Her pipeline asamasinda ortaya cikan veri, Graphiti'ye episode olarak
batch halinde gonderilir. Graphiti her episode icin 29.4.3'teki 6 adimli
isleme zincirini otomatik calistirir:

```python
# OpenFang → Graphiti entegrasyonu (MCP veya subprocess)

from graphiti_core import Graphiti
from graphiti_core.driver.kuzu_driver import KuzuDriver
from graphiti_core.nodes import EpisodeType
from graphiti_core.llm_client import OpenAIClient  # veya AnthropicClient

# Baslangicta: Kuzu embedded DB ile offline graf olustur
driver = KuzuDriver(db='~/.openfang/data/prospect_graph.kuzu')
graphiti = Graphiti(driver=driver, llm_client=OpenAIClient())

# Pipeline sirasinda: Episode ekle
await graphiti.add_episode(
    name="TMB member: Alarko",
    episode_body="ALARKO CONTRACTING GROUP, Chairman: Izzet Garih, web: alarko.com.tr",
    source=EpisodeType.text,
    source_description="TMB members directory scrape",
    reference_time=datetime.now(),
    group_id="prospect_run_2026_03_26"
)
# Graphiti OTOMATIK olarak:
# → "Alarko" Account entity cikarir/esler
# → "Izzet Garih" Contact entity cikarir
# → EMPLOYS edge olusturur (valid_at=now)
# → Community gunceller ("TR Insaat Muteahhitleri")

# Bounce geldiginde:
await graphiti.add_episode(
    name="Bounce: old@alarko.com.tr",
    episode_body='{"type": "hard_bounce", "email": "old@alarko.com.tr"}',
    source=EpisodeType.json,
    source_description="SMTP bounce notification",
    reference_time=datetime.now(),
)
# → REACHABLE_VIA edge invalidate edilir (invalid_at=now)

# Arama:
results = await graphiti.search(
    query="insaat sektoru CEO'lari Turkiye",
    group_ids=["prospect_run_2026_03_26"],
    num_results=20
)
# → facts + entities + communities doner
```

**Gercek pipeline ornekleri:**
```
TMB taramasi → 8 uye buldu
  → 8 text episode, Graphiti.add_episode() ile batch
  → Graphiti: entity extraction + resolution + community update

Site HTML zenginlestirme → "Izzet Garih, Chairman" buldu
  → 1 text episode
  → Graphiti: Contact entity esler, EMPLOYS edge olusturur

Bounce geldi
  → 1 json episode
  → Graphiti: REACHABLE_VIA edge invalidate (invalid_at=now)

Kullanici duzenleme: "Bu kisi artik CEO degil"
  → 1 message episode
  → Graphiti: eski EMPLOYS fact invalidate, yeni fact olustur
```

#### 29.4.7 Avantajlar vs Mevcut SQLite Modeli

| Ozellik | Mevcut (SQLite JSON) | Graphiti + Kuzu Embedded |
|---------|---------------------|------------------------|
| Iliski takibi | Yok (flat tablo) | Tam graf: Entity → RelatesToNode_ → Entity |
| Zamansal degisim | Upsert (eski kaybolur) | Bi-temporal: valid_at/invalid_at + created_at/expired_at |
| Entity resolution | Domain bazli dedup | Embedding cosine + fulltext + LLM dogrulama |
| Arama | SQL LIKE | Cosine similarity + BM25 fulltext + BFS graf yurume |
| Buying committee | Tek primary_contact | Coklu Contact per Account, her biri ayri entity |
| Community/kumeleme | Yok | Label propagation ile otomatik, dinamik guncelleme |
| Kaynak izleme | Yok | Episode → Entity cift yonlu index (provenance) |
| Depolama | Tek SQLite dosyasi | Kuzu dosya-bazli DB (sunucusuz, SQLite gibi) |
| Dis bagimllik | Yok | graphiti-core Python + kuzu embedded (tek dosya) |
| Sinyal birikimi | matched_signals[] string | Episode → fact → entity zinciri |
| Kaynak izleme | Yok | Episode → entity cift yonlu index |
| Community/kumeleme | Yok | Otomatik label propagation |
| Holding/istirak | Yok | Account → Account iliskileri |
| Olceklenebilirlik | Tek SQLite dosyasi | Neo4j backend, dagitik |

### 29.5 Decision Engine (5 Eksenli)

```
FitScore:
  sektor_esleme * 0.3
  + buyukluk_esleme * 0.2
  + cografya_esleme * 0.2
  + site_icerik_esleme * 0.15
  + dizin_uyelik * 0.15

IntentScore:
  yeni_tesis_sinyali * 0.3
  + ihale_sinyali * 0.3
  + buyume_sinyali * 0.2
  + web_aktivite * 0.2

ReachabilityScore:
  kisisel_email * 0.35
  + linkedin_profil * 0.25
  + telefon * 0.2
  + gercek_kisi_adi * 0.1
  + dogrulanmis_unvan * 0.1

DeliverabilityRisk:
  bounce_gecmisi * 0.3
  + domain_itibari * 0.3
  + generic_email * 0.2
  + sender_health * 0.2

ComplianceRisk:
  suppression_listesi * 0.4
  + opt_out_gecmisi * 0.3
  + kvkk_risk * 0.3
```

**Send Gate karari:**
- Block: DeliverabilityRisk > 0.7 VEYA ComplianceRisk > 0.5
- Research: ReachabilityScore < 0.3
- Nurture: IntentScore < 0.2
- Ready to Activate: FitScore > 0.5 VE ReachabilityScore > 0.4 VE Risk < 0.5

### 29.6 Sequence Planner

Tek mesaj degil, **sekans**:

```
Touch 1: Kisa e-posta (kanit bazli, kisisellestirmis)
Touch 2: Deger icerigi (teardown / case study)
Touch 3: Follow-up ("gecen hafta..." referans)
Touch 4: LinkedIn manuel asist VEYA arama gorevi
Touch 5: Son nazik kapanis

Zamanlama: Segment, persona, kanal, ilk temas sonucu,
domain turu ve onceki engagement'a gore DINAMIK.
Sabit 7/14 gun degil.
```

### 29.7 Evidence-Bound Message Engine

LLM mesaj uretimi kontrollü olmali:

```
Girdi:
  evidence_bundle: [kanit_1, kanit_2, ...]
  persona: {isim, unvan, sektor, aci_noktasi}
  sequence_step: 1
  language: "tr"

Cikti:
  subject: "..."
  body: "..."
  linkedin_copy: "..." (sadece manuel asist notu)
  claims: ["iddia_1", "iddia_2"]
  evidence_ids: ["ev_123", "ev_456"]
  risk_flags: ["generic_email", "unverified_name"]
  language_confidence: 0.95
  similarity_score: 0.12  // onceki mesajlara benzerlik
```

Her mesaj: iddialarini hangi kanita dayandirdigini soyler.
Spammy phrasing kontrolu + compliance footer + opt-out + 2 varyant.

### 29.8 Sendability / Compliance Guardrail

**Email tarafinda:**
- Sender mailbox pool (tek adres degil)
- Brand domain / sending subdomain ayrimi
- Mailbox warm-state takibi
- Daily cap PER MAILBOX (global degil)
- Per-domain throttle
- Bounce/complaint suppression
- Unsubscribe ledger
- SPF + DKIM + DMARC zorunlu
- 0.3% alti spam orani hedefi

**KVKK / Ticari Elektronik Ileti:**
- Tacir/esnaf alicilara onceden onay istisnasi VAR
- AMA ret hakki kullanildiktan sonra tekrar ileti gonderilemez
- Onay istemek icin ayrica ticari ileti atilamaz
- opt-out, suppression, retention ve purpose logging mimarinin MERKEZINDE

### 29.9 Learning Loop (Feedback Brain)

Sistem sunlari ingest etmeli:

| Olay | Siniflandirma | Aksiyon |
|------|---------------|---------|
| Hard bounce | deliverability_fail | Suppression + score guncelle |
| Soft bounce | deliverability_warn | Retry sonra suppress |
| Auto reply | neutral | Parse + log |
| Unsubscribe | compliance_action | Kalici suppress |
| Positive reply | outcome_positive | Meeting akisina al |
| Referral | outcome_redirect | Yeni contact olustur |
| Wrong person | enrichment_fail | Contact guncelle |
| Not now | timing_miss | Nurture kuyuguna al |
| Interested | outcome_warm | Hizlandir |
| Meeting booked | outcome_success | CRM sync |

Bu veriler 3 yere akar:
1. **Suppression list** — bir daha gonderme
2. **Score calibration** — hangi sinyaller gercekten ise yariyor?
3. **Prompt/sequence experimentation** — hangi mesaj/sekans daha iyi?

---

## 30. GUNCEL IMPLEMENTASYON PLANI (v4 — ChatGPT Pro + Gemini + Gemini Deep Research birlesmis)

> Bu plan, uc ayri uzman degerlendirmesinin sentezlenmis halidir:
> - ChatGPT Pro: Mimari yeniden yapilandirma (account graph, 3 motor ayrimi, ogrenme dongusu)
> - Gemini: Taktiksel detaylar (PipelineStage trait, tier-based kesif, 2-asamali mesaj, UI sekmeleri)
> - Gemini Deep Research: Stratejik vizyon (niyet motoru, multi-agent swarm, deger uretimi, deliverability zirhi, omnichannel durum makinesi)
> Catisma durumunda daha spesifik/uygulanabilir olan tercih edilmistir.

### Faz 0: Kan Kaybini Durdur (0-7 Gun)

| # | Is | Detay | Neden Acil |
|---|------|-------|-----------|
| 1 | Consumer domain gate TUM kod yollarinda | `is_valid_company_domain()` fonksiyonu: `!is_consumer_email_domain(d) && !is_blocked_company_domain(d)` — her dizin tarayici ciktisinda zorunlu | Spam riski (gmail.com prospect) |
| 2 | `normalize_and_classify_email()` gateway | Tek giris noktasi: Personal / Generic / Role / Consumer / Invalid siniflandirmasi. info@ = Generic, lead olusturma oncesi TUM kod yollarinda zorunlu | info@ lead %0 |
| 3 | Turkce placeholder listesi genislet | +8 terim: "baskanin mesaji", "genel mudurun mesaji", "hakkimizda", "vizyonumuz", "misyonumuz", "iletisim", "kariyer", "basin" | Sahte kisi adi |
| 4 | Telefon aktarimi | `phone: None` → `phone: candidate_phone.clone()` + `FreeDiscoveryCandidate`'e `phone: Option<String>` alani | Kayip veri (~15 telefon) |
| 5 | Idempotency key | `account_domain + contact_name + channel + sequence_step` UNIQUE partial index + outbox pattern | Cift gonderim |
| 6 | LinkedIn auto-send KAPAT | `send_linkedin()` → sadece operator dashboard'da "LinkedIn'de su mesaji gonder" gorevi olustur, tarayici otomasyonu kaldir | TOS ihlali riski |
| 7 | Async job + stage checkpointing | `run_generation()` icin `PipelineStage` trait: her asama DB'ye state yazar, hata durumunda sadece o asama retry edilir, API sadece job_id dondurur + progress endpoint | 240sn timeout |
| 8 | Suppression tablosu + unsubscribe ledger | `suppressions(contact_method_id, reason, suppressed_at)` + her teslimat oncesi kontrol | Compliance temeli |
| 9 | Field-level confidence semasi | company_name_confidence, domain_confidence, contact_name_confidence, title_confidence, email_confidence, linkedin_confidence, phone_confidence — her alan ayri | Veri kalitesi |
| 10 | Frontend/backend target_geo duzeltme | Rust default "US" → "TR" veya JS default kaldir, tek kaynak sema | Schema drift |
| 11 | Scraper health check | Her tarayici 0 sonuc dondurse alert + sonraki run'da auto-skip + parse_health metrigi | Sessiz kirilma |
| 12 | Gonderim oncesi Bounce Shield | Her e-posta gonderilmeden once MX kayit kontrolu + SMTP VRFY ping. Gecemeyen (catch-all, gecersiz) mailler KESINLIKLE reddedilir, hedef LinkedIn manuel asist'e kaydirilir | Blacklist onleme |
| 13 | Gelen kutusu rotasyonu altyapisi | Ana domain (`@machinity.com`) ile dogrudan gonderim YASAKLA. Alt domainler (`getmachinity.com`, `trymachinity.com` vb.) uzerinden 3-5 mailbox ile yuk dengeleme. Her mailbox gunluk cap + warm-up state takibi | Domain itibari |

### Faz 1: Revenue Cekirdegi (7-21 Gun)

| # | Is | Detay | Etki |
|---|------|-------|------|
| 14 | Account graph — Graphiti + Kuzu embedded | `graphiti-core` (Apache 2.0, v0.28.2) acik kaynak kutuphanesi + `KuzuDriver(db='~/.openfang/data/prospect_graph.kuzu')` embedded graf DB. 3 katmanli hiyerarsi (Episode → Semantic Entity → Community), bi-temporal model, otomatik entity/fact extraction + resolution + invalidation. OpenFang entegrasyonu: Graphiti MCP server modu veya Python subprocess. Dis bulut servisi YOK, tamamen offline. Detaylar: Bolum 29.4 | Lead flat tablo → temporal account graph |
| 13 | Discovery ve Activation ayirimi | Discovery Reservoir (surekli doldurulan hesap evreni) + Research Queue (eksik kimlik tamamla) + Activation Queue (bugun gonderilecek en iyiler). `daily_target` sadece gonderimi sinirlar, kesfii degil | Dogru optimizasyon |
| 14 | 5 eksenli puanlama | FitScore, IntentScore, ReachabilityScore, DeliverabilityRisk, ComplianceRisk — formulleri Bolum 29.5'te | Gercek karar kalitesi |
| 15 | Send Gate mantigi | Block (risk>0.7), Research (reach<0.3), Nurture (intent<0.2), Ready (fit>0.5 & reach>0.4 & risk<0.5) | Risk bazli routing |
| 16 | Reply/bounce classification ingest | hard_bounce, soft_bounce, auto_reply, unsubscribe, positive_reply, referral, wrong_person, not_now, interested, meeting_booked — her biri suppression/score/experiment'a akar | Ogrenme temeli |
| 17 | Sender pool + mailbox policy | Birden fazla gonderen mailbox, daily cap PER MAILBOX, warm-state takibi, brand domain / sending subdomain ayrimi | Deliverability |
| 18 | Evidence provenance + kaynak guvenilirlik hiyerarsisi | Her veri parcasinin kaynagi + guveni. Cakisma cozumu: Dizin uyelik=0.9, Site HTML=0.8, Web arama=0.6, LLM uretimi=0.4. Yuksek guven kazanir | Denetlenebilirlik |
| 19 | Tier-based kesif modeli | Tier 1 (5 sirket): tam zenginlestirme (site+OSINT+LLM+LinkedIn+tel+email pattern). Tier 2 (10): standart (site+OSINT+LLM). Tier 3 (5): temel bilgi. Kullanici Tier'i gorur | Derinlik > genislik |
| 20 | 4 katmanli LinkedIn arama | 1: `site:linkedin.com/company/ "{domain_adi}"` 2: `"{sirket_transliterated}"` 3: `"{sirket}" linkedin CEO` 4: `site:tr.linkedin.com "{domain}"` — basarisiz olana kadar sirala | %0 → %30 LinkedIn |
| 21 | E-posta pattern tahmin motoru | ad.soyad@, a.soyad@, adsoyad@, ad@ + MX kayit kontrolu + opsiyonel SMTP VRFY. Kisi adi bilinip email yoksa otomatik calisir | %0 → %20 kisisel email |
| 22 | Turkce site sayfa genisletmesi | /yonetim, /ekibimiz, /yonetim-kurulu, /iletisim, /bize-ulasin, /referanslarimiz, /projelerimiz, /haberler + sitemap parsing onceliklendirme | Daha fazla veri |
| 23 | Niyet motoru: Is ilani sinyalleri | Kariyer.net / LinkedIn Jobs taramasi: Hedef firma "Saha Operasyon Yoneticisi" ariyorsa operasyonel aci noktasi var. IntentScore'a +500 puan. En guclu miknatıs cekimi | Statik veri → canli niyet |
| 24 | Niyet motoru: Tech-stack dedektifi | Wappalyzer/BuiltWith mantigi ile hedef sitelerin DOM taramas. Rakip urun kullaniliyor mu? Strateji "Rakip Degistirme (Rip & Replace)" acisina doner | Rekabet avantaji |

### Faz 2: Ogrenen Sistem (21-45 Gun)

| # | Is | Detay | Etki |
|---|------|-------|------|
| 23 | 2 asamali LLM mesaj uretimi | Asama 1: Strateji (hangi aci noktasi, hangi kanit, hangi CTA?). Asama 2: Yazim (maks 120 kelime, sirket-ozel gancho, temperature 0.4-0.6). Her mesaj: claims[] + evidence_ids[] + risk_flags + similarity_score | Mesaj kalitesi |
| 24 | Sequence planner | 5 adimli sekans: (1) kisa email (2) deger icerigi (3) follow-up (4) LinkedIn manuel asist / arama gorevi (5) son kapanis. Zamanlama segment/persona/kanal/engagement'a gore DINAMIK | Tek mesaj → sekans |
| 25 | Outcome-based score calibration | Reply/bounce/meeting verisi → hangi sinyaller gercekten ise yariyor? Puanlama agirliklarini sonuca gore ayarla | Puanlarin dogrulugu |
| 26 | Experiment registry + prompt/sequence versioning | Her prompt ve sekans versionlanir. A/B: 2 varyant konu satiri + govde. Hangi varyant secildi + sonucu loglanir | Optimizasyon |
| 27 | Source pack health metrikleri | Her kaynak icin: precision, freshness, parser_health, legal_mode, historical_reply_yield. 0 sonuc = alert + auto-skip | Kaynak kalite |
| 28 | Dossier builder (kanit-bagli) | company thesis, why-now thesis, buyer committee map, top 3 pains, top 3 proofs, objections, do-not-say. Her cumlenin altinda evidence reference | Derinlik |
| 29 | LLM hallucination dogrulama | LLM sirket uretiminden sonra: HEAD request (200=gercek), WHOIS domain yasi, LLM puani web/dizinden dusuk tutma (0.4 vs 0.8). Alternatif: LLM sorgu uretsin, sonucu web'den al | Veri guveni |
| 30 | Islem bazli model secimi + multi-agent swarm | Veri cekimi: `gpt-4o-mini` (ucuz, hizli). Mesaj yazimi: `claude-sonnet` (empati, ritim). Strateji/dogrulama: `gpt-5.3-codex`. OpenFang Agent Loop ile 4 uzman ajan: **Arastirmaci** (headless browser + DOM→Markdown→JSON, regex'i devre disi birakir), **Psikolog** (karar verici LinkedIn ayak izinden kisilik profili: Titiz/Analitik → ROI ve sayi odakli veri, Vizyoner → buyuk resim anlatimi — mesaj tonu buna gore ayarlanir), **Yazar** (claude-sonnet ile kisisellestirmis mesaj, asla sablona benzemeyen gancho), **Uyum** (spam kelime kontrolu, uzunluk, ton, benzerlik skoru — basarisizsa Yazar'a iade eder) | Kalite + maliyet + uzmanlik ayrimi + kisilik-bazli ton |
| 31 | Dinamik deger uretimi (miknatıs yemi) | Hedef sirkete ozel **Mikro-Rapor PDF** uretimi: LLM ile sirket verilerinden 2 sayfalik "Operasyonel Darboğaz Analizi" olustur (OpenFang `image_generate` + HTML→PDF template). Icerik: sirketin sektoru + tespit edilen aci noktalari + benchmark karsilastirma + 3 adimlik iyilestirme onerisi. Mail govdesinde sadece teaser: "Alarko'ya ozel 2 sayfalik operasyonel analiz simülasyonu hazirladik. Gondereyim mi?" — musteri "Gonder" dediginde cekim alanina girmis olur. **Bu asla satis maili degil, deger sunumudur.** | Push → Pull donusumu |
| 32 | Geri besleme dongusu: Ton ogrenme (RLHF + Vector Memory) | Kullanici mesaj uzerinde kelime degisikligi yaptiginda (ozellikle buzkırıcı/gancho duzenlemesi), degisiklik OpenFang `SemanticStore`'a `(orijinal, duzenlenmis, baglam)` uclus olarak kaydedilir. Sonraki calismalarda Yazar Ajan, kullanicinin tercih ettigi stili (Tone of Voice) RAG mimarisiyle taklit etmeye baslar. Ek: Hizli onay ekraninda Sag Ok (onayla), Sol Ok (reddet), Yukari Ok (yeniden yaz) aksiyonlari da tercih verisi olarak loglanir — hangi mesajlar onaylandi, hangileri reddedildi, hangileri yeniden yazdirildi | Zaman icinde kisisellesen + operator tercihinden ogrenen sistem |
| 33 | Omnichannel durum makinesi detayi | OpenFang WorkflowEngine ile: Gun 1: LinkedIn profil ziyareti (sessiz) → Gun 2: Deger e-postasi → Gun 5: Acildi ama yanitlanmadiysa LinkedIn baglanti istegi → Gun 8: Follow-up email → Gun 12: Son kapanis. Her gecis olay-gudumlü (pixel acilma, bounce, reply) | Tek atis → coklu temas |

### Faz 3: UI/UX Donusumu (30-50 Gun, Faz 2 ile paralel)

| # | Is | Detay | Etki |
|---|------|-------|------|
| 31 | Sekme bazli navigasyon | 4 sekme: Komuta Merkezi (ozet+hizli eylem) / Profiller (master-detail) / Onay Kuyrugu (toplu islem) / Teslimat (analitik) | Odakli is akisi |
| 32 | 8 kartli dashboard | +Email Only (sari) + Ortalama Kalite Puani (0-1000, trend oku) ayrimlari. info@ "Contact Ready" dan cikarilir | Dogru veri |
| 33 | Puan breakdown bileseni | Dossier'de 4 eksenli yatay cubuk grafik: Temel ██░░ 80/200, Iletisim ████ 200/300, ICP ██████ 240/300, Sinyal ███░ 100/200 | Seffaflik |
| 34 | Toplu onay/reddet | Checkbox + "Secilileri Onayla" + "Tum 750+ puanlilari onayla" akilli filtre butonu | UX hizi 10x |
| 35 | Inline mesaj duzenleme | Onay kartinda mesaj govdesi tiklanabilir textarea. "Duzenlenmis Onayla" butonu. `edited_payload` JSON alani | Esneklik |
| 36 | info@ uyari kutusu | Sari uyari: "Bu genel bir e-posta adresidir. Yanit orani dusuk olabilir." + dusuk puan (<400) uyarisi | Farkindalik |
| 37 | Baglam-duyarli aksiyon onerileri | contact_ready+yuksek: "Hemen email gonderin". email_only: "LinkedIn'de {title} arayin". contact_identified+tel: "Telefon ile ulasin: {phone}" | Aksiyon netligi |
| 38 | Hizli onay modu (Tinder-style) | Dev "Profil + Mesaj Karti" gorunumu. Klavye kisayollari: Sag Ok = Onayla, Sol Ok = Reddet, Yukari Ok = LLM'e yeniden yazdır. 100 lead'i 3 dakikada eritme | Onay hizi 20x |
| 39 | Buzkırıcı (Icebreaker) editoru | Mesajin tamamini degil, LLM'in urettigi ilk "kisisellestirmis cumleyi" hizlica duzenleme alani. Geri kalan mesaj sabit, sadece gancho degisir | Hassas kontrol |

### Faz 4: Olcekleme (60-90 Gun)

| # | Is | Detay | Etki |
|---|------|-------|------|
| 40 | First-party intent collector + tersine IP | Site ziyaret, demo form, webinar, iceric indirme, urun deneme, eski CRM hareketi, LinkedIn sirket sayfasi etkilesimi, gelen reply/referral. **Ek: Tersine IP sinyali (de-anonymization)** — web sitesine eklenen script ile ziyaretci sirket agini tespit et (Clearbit/Leadfeeder mantigi), dogrudan sicak hedef olarak account graph'a ekle ve IntentScore'u yuksel | Inbound + outbound + anonim ziyaretci yakalama |
| 41 | ICP control plane | Coklu ICP tanimlari, alt segmentler, negatif ICP kurallari, persona haritasi, mesaj stratejileri, sender policy, scoring version, prompt version | Strateji yonetimi |
| 42 | CRM sync (HubSpot/Pipedrive) | Webhook bazli cift yonlu sync. Account+contact+activity gonderimi | Is akisi |
| 43 | Canonical account dedup | Ad benzerligi + site kimligi + iletisim sinyali + kaynak dogrulama. canonical_account, account_alias, domain, location, legal_entity_name, brand_name | Veri hijyeni |
| 44 | Compliance dashboard | KVKK durum takibi, suppression listesi yonetimi, retention suresi, purpose logging, opt-out gecmisi | Yasal guvence |
| 45 | Operator auto-approve segments | Risk bazli routing: auto-block (acik riskli), research-needed (generic/dusuk guven), manual-review (yuksek deger+risk), auto-send (kanitli+guvenli segment) | Olcek |
| 46 | LLM-assisted scraper extraction | Regex yerine HTML → LLM → yapilandirilmis veri. HTML degisikliklerine dayanikli. Orta vadeli regex yedegi korunur | Kirilganlik azaltma |

---

### Faz Bazli Beklenen Metrikler

| Metrik | Mevcut | Faz 0 Sonrasi | Faz 1 Sonrasi | Faz 2-3 Sonrasi | Faz 4 Sonrasi |
|--------|--------|--------------|--------------|--------------|----------------|
| Gecersiz domain | %3 | %0 | %0 | %0 | %0 |
| info@ lead orani | %80 | %0 (onayda) | %0 | %0 | %0 |
| LinkedIn bulma | %0 | %0 | %30+ | %30+ | %35+ |
| Kisisel e-posta | %0 | %0 | %20+ | %20+ | %25+ |
| Telefon | %0 | %50+ | %50+ | %50+ | %50+ |
| Puan ayristirma | 30/32=100 | Hala eski | Normal dagilim | Kalibre edilmis | Optimized |
| Mesaj kisisellestirme | %0 sablon | %0 | %0 | %100 LLM | %100 + A/B |
| E-posta acilma | Bilinmiyor | Bilinmiyor | Bilinmiyor | ~%20 | %25+ |
| E-posta yanit | Bilinmiyor | Bilinmiyor | Bilinmiyor | ~%3 | %5+ |
| Kullanici guveni | Dusuk | Orta | Yuksek | Yuksek | Cok Yuksek |

---

## 31. OpenFang Platformundan Kullanilacak Parcalar

| OpenFang Parcasi | Kullanim |
|-----------------|----------|
| WorkflowEngine | Stage orchestration (discovery → research → activation) |
| Knowledge Graph / Memory | Account graph + relationship memory |
| Approval System | Risk-based review (Send Gate) |
| Audit Log | Send/reply/compliance trail |
| MeteringEngine | LLM/task cost visibility |
| Model Catalog | Provider abstraction |
| EventBus | Olay bazli tetikleme (reply geldi → score guncelle) |
| CronScheduler | Gunluk kesif + gonderim zamanlama |

**Core path'ten CIKARILACAKLAR:**
- A2A/OFP mesh (satis icin gereksiz karmasiklik)
- 40 kanal fantazisi (email + LinkedIn manual yeterli)
- Serbest agent autonomy (typed workflow daha guvenli)

---

## 32. Veri Modeli Gecis Plani

### Mevcut (8 tablo, SQLite, JSON blob agirlikli)
```
sales_profile → JSON blob
prospect_profiles → JSON blob
leads → flat tablo
approvals → flat tablo
deliveries → flat tablo
```

### Hedef: Hibrit Model (Graphiti/Kuzu + SQLite)

**Graphiti + Kuzu Embedded — Account Graph:**
```
graphiti-core acik kaynak kutuphanesi (Apache 2.0) + Kuzu embedded graf DB.
Dosya-bazli, sunucusuz — ~/.openfang/data/prospect_graph.kuzu
Dis bulut servisi yok, tamamen offline calisir.

Episode Subgraph (Ge):
  - Ham girdi verileri (text, json, message)
  - Kayipsiz (non-lossy) depolama
  - Kaynak izlenebilirligi

Semantic Entity Subgraph (Gs):
  Account (canonical_id, display_name, legal_name, sector, geo, employee_est)
  Contact (full_name, title, seniority, department)
  Domain (domain, is_primary, verified)
  ContactMethod (type, value, confidence, verified_at)
  Signal (type, text, source, observed_at)
  Product (name, category — tech-stack tespiti icin)
  Event (type, date — ihale, tesis, sertifika)

  Semantic Edges (zamansal fact'ler):
  EMPLOYS (valid_at, invalid_at, title)
  HAS_DOMAIN (valid_at, invalid_at)
  REACHABLE_VIA (channel_type, confidence, verified_at)
  OBSERVED_SIGNAL (observed_at, source)
  USES_PRODUCT (detected_at, source)
  PARTICIPATED_IN (date)
  SENT_TO (sequence_step, sent_at)
  REPLIED_TO (type, received_at)

Community Subgraph (Gc):
  - Otomatik kumeleme (label propagation)
  - Sektor bazli community'ler: "TR Insaat", "Makine Sanayi"
  - Community ozetleri (map-reduce)
```

**SQLite (sales.db) — Operasyonel Veriler (korunan):**
```
-- Strateji (YENi)
icp_definitions
segments
personas
sender_policies

-- Aktivasyon (YENi)
campaigns (icp_id, segment_id, status)
sequence_instances (campaign_id, account_graph_id, contact_graph_id, current_step)
touch_instances (sequence_id, step, channel, message_id, sent_at)

-- Teslimat (mevcut, genisletilmis)
send_events (touch_id, status, mailbox_id, sent_at)
reply_events (touch_id, type, classified_at, raw_text)
suppressions (contact_method_value, reason, suppressed_at)

-- Deney (YENi)
experiments (name, hypothesis, variant_a, variant_b, status)
experiment_assignments (experiment_id, sequence_id, variant)

-- Mevcut (korunan, temporal KG gecis surecinde)
sales_profile, sales_runs, approvals, deliveries, sales_onboarding
```

**Gecis Stratejisi:**
- `prospect_profiles` ve `leads` → Graphiti account graph'a tasinir
  (her mevcut prospect icin bir text episode olustur, Graphiti otomatik isle)
- `discovered_domains` → Graphiti entity resolution ile degistirilir
- `sales_profile` → `icp_definitions` + `sender_policies`'e evrilir
- `approvals` ve `deliveries` SQLite'ta kalir (operasyonel, hizli erisim)
- Graphiti entity UUID'leri SQLite kayitlarinda referans olarak saklanir
- Graphiti Kuzu DB dosyasi: `~/.openfang/data/prospect_graph.kuzu`

---

## SON SOZ

Bu dokuman 4 parcadan olusur:

1. **Parca 1:** OpenFang platformunun tum alt sistemleri (kernel, agent, bellek, A2A/OFP, butce, workflow, 40 kanal)
2. **Parca 2:** Prospecting motorunun birebir teknik referansi (38 sabit, 8 struct, 8 DB tablosu, 7 asamali pipeline, 8 dizin tarayici, 14 filtreleme fonksiyonu, 5 LLM prompt, SMTP/LinkedIn kodu, tam JS/HTML analizi)
3. **Parca 3:** Ilk hedef mimari + ekran tasarimlari + 12 bug analizi
4. **Parca 4:** Uzman degerlendirmesi (ChatGPT Pro + Gemini + Gemini Deep Research) sonrasi birlesmis mimari — Revenue Graph + Activation Engine + Learning Loop + Multi-Agent Swarm + Niyet Motoru + Deger Uretimi + **53 maddelik, 5 fazli implementasyon plani**

**Temel donus:**
Sistem "daha cok kaynak eklenerek" degil, "karar katmani ve ogrenme katmani guclendirilerek" mukemmellesir.
Lead tablosundan account graph'a, tek pipeline'dan discovery-activation-learning ayrimina,
SMTP+LinkedIn otomasyonundan sendability/compliance/reply-intelligence duzeyine gecis gerekiyor.

**Gercek musteri bulma miknatisi:** en cok aday bulan sistem degil,
**en guvenilir sekilde en cok olumlu cevabi ureten sistemdir.**
