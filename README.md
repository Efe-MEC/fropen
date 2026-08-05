# ✈️ Gerçek Zamanlı Uçuş Takip ve Veri Analizi Sistemi

Bu proje, OpenSky Network API üzerinden alınan gerçek zamanlı uçuş telemetri verilerinin kesintisiz bir şekilde toplanmasını, işlenmesini, depolanmasını ve son kullanıcıya canlı bir harita üzerinden sunulmasını sağlayan **uçtan uca bir veri mühendisliği** mimarisidir. Büyük veri staj projesi kapsamında geliştirilmiştir.

---

## 🏗️ Kullanılan Teknolojiler ve Servisler

Proje kapsamında verinin üretilmesinden son kullanıcıya ulaştırılmasına kadar çeşitli modern veri ve web teknolojileri kullanılmıştır:

*   **Apache Kafka (KRaft Modu):** Veri akışının ana omurgası. Dağıtık veri iletimi için 3 broker'lı cluster mimarisi.
*   **MongoDB:** Ham uçuş verilerini (`raw_flights`) ve uçuş seanslarını (`flight_sessions`) depolayan NoSQL veritabanı.
*   **Python (FastAPI & Uvicorn):** Yüksek performanslı asenkron REST API ve WebSocket sunucusu.
*   **Pandas & PyArrow:** Geçmiş verilerin günlük işlenmesi ve `.parquet` formatında sıkıştırılarak arşivlenmesi.
*   **Docker & Docker Compose:** Tüm bileşenlerin izole edilmesi ve orkestrasyonu (Ubuntu ortamında).
*   **Web Frontend (HTML/JS/Leaflet):** Canlı harita çizimi ve veri filtreleme işlemleri için kullanıcı arayüzü.

---

## 🔄 Sistem Mimarisi ve Veri Akışı (Data Pipeline)

Sistem mimarisi aşağıdaki veri işleme boru hattından (pipeline) oluşur:

1.  **Veri Üretimi (Ingestion):** `flight-producer` servisi, OpenSky API'den canlı uçuş verilerini çeker ve Kafka'nın `flight-data` topiğine asenkron olarak gönderir.
2.  **Veri Taşıma (Streaming):** Kafka Cluster, gelen verileri hata toleranslı ve yüksek erişilebilir bir şekilde taşır.
3.  **Veri Tüketimi ve Kayıt (Processing & Storage):** `flight-consumer` servisi Kafka topiğini dinler. Verileri MongoDB'ye kaydeder ve aktif uçuş rotalarını çizmek için `flight_sessions` koleksiyonunu günceller.
4.  **Veri Sunumu (Serving):** `flight-api` servisi, MongoDB'den geçmiş verileri okur ve Kafka'dan akan canlı verileri WebSocket aracılığıyla anlık olarak frontend'e iletir.
5.  **Soğuk Depolama (Archiving):** `flight-archiver` servisi, her gece çalışarak (cron job) bir önceki günün verilerini MongoDB'den çeker ve `Snappy` sıkıştırmalı Parquet dosyaları olarak partition'lara ayırıp diske kaydeder.

---

## 🧩 Program Şeması ve Modüller

*   **Producer (`producer.py`):** Durum vektörlerini parse edip JSON payload'u oluşturur. Uçuşun `icao24` kodunu Kafka key'i olarak kullanır.
*   **Consumer (`consumer.py`):** Verileri çekerken MongoDB üzerinde TTL (Time-To-Live) indeksleri oluşturur. İnmiş (on_ground) veya 30 dk boyunca sinyal alınamamış uçuşların oturumunu sonlandırır.
*   **FastAPI Backend (`frontend.py`):** REST API üzerinden geçmiş ve havaalanı verilerini sunar. `aiokafka` ile asenkron Kafka dinleyicisi çalıştırarak WebSocket üzerinden canlı veri yayını (broadcast) yapar.
*   **Arşivleme (`archive.py`):** Veritabanı maliyetlerini düşürmek için `schedule` kütüphanesi ile tetiklenir, `year=.../month=.../day=...` partition yapısıyla arşivleme yapar.

---

## 🚀 Kurulum ve Çalıştırma Yolu (Deployment)

Proje baştan aşağıya mikroservis mimarisine uygun olarak Dockerize edilmiştir. Tüm servislerin doğru ağ (network) konfigürasyonu ile çalışması için `docker-compose.yml` yapılandırılmıştır.

### 1. Ortam Değişkenlerini Ayarlama
Proje dizininde bir `.env` dosyası oluşturun ve aşağıdaki değişkenleri tanımlayın:

```env
MONGO_USERNAME=admin
MONGO_PASSWORD=password123
MONGO_URI=mongodb://admin:password123@mongodb:27017/
OPENSKY_CLIENT_ID=your_id
OPENSKY_CLIENT_SECRET=your_secret
```

### 2. Imaj Dosyasini Yükleme ve Konteynerları Ayağa Kaldırma
Docker Engine yüklü ortamınızda (örn. Ubuntu terminali), imajları derlemek ve servisleri başlatmak için şu komutu çalıştırın:

```bash
docker load -i fropen.tar
```

```bash
docker-compose up -d --build
```

### 3. Uygulamaya Erişim
*   **Web Arayüzü (API ve Harita):** `http://localhost:8000`
*   **Veritabanı Yönetim Paneli (Mongo Express):** `http://localhost:8081`

> **Not:** Sistem ayağa kalktığında `airport-seeder` modülü havalimanı verilerini MongoDB'ye otomatik olarak aktaracaktır.

---

## 💡 Kazanımlar (Öğrenim Çıktıları)

Bu proje geliştirilirken aşağıdaki konularda pratik tecrübe edinilmiştir:
*   **Konteyner Orkestrasyonu:** Docker Compose ile izole edilmiş mikroservislerin tek bir ağda haberleştirilmesi.
*   **Apache Kafka (KRaft):** Zookeeper bağımlılığı olmadan cluster kurulumu, partition/offset yönetimi.
*   **Büyük Veri Arşivleme:** Pandas ve PyArrow kullanarak büyük verilerin analitik süreçler için Parquet formatında partitionlanması.
*   **Asenkron Streaming:** FastAPI ve WebSocket üzerinden canlı verilerin darboğaz (bottleneck) yaratmadan frontend'e anlık iletimi.

---
*Hazırlayan: Mehmet Efe Çakır*