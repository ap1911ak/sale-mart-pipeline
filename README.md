# 🛒 Sale Mart Pipeline (Data Engineering Project)

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Container-Docker-2496ED.svg)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-336791.svg)](https://www.postgresql.org/)

Sale Mart Pipeline คือโปรเจกต์ Data Engineering ที่ออกแบบมาเพื่อจัดการระบบ ETL (Extract, Transform, Load) แบบอัตโนมัติ โดยนำข้อมูลการขายจากระบบ Sale Mart มาประมวลผลและจัดเก็บลงใน Data Warehouse เพื่อรองรับการทำ Business Intelligence

---

## 📌 ภาพรวมโปรเจกต์ (Project Overview)

โปรเจกต์นี้เปลี่ยนจากการรัน Script ด้วยมือ (Manual) มาเป็นการใช้ **Orchestration Tool** อย่าง Airflow เพื่อควบคุม Workflow และใช้ **Docker** เพื่อจัดการ Environment ให้ทำงานได้เหมือนกันในทุกเครื่อง



### Key Components:
1.  **Extract:** ดึงข้อมูล CSV/JSON จาก Source สู่ Staging Area
2.  **Transform:** ประมวลผลข้อมูลด้วย Pandas (Data Cleaning, Handling Missing Values)
3.  **Load:** โหลดข้อมูลเข้าสู่ PostgreSQL ในรูปแบบ Star Schema
4.  **Orchestration:** ควบคุมลำดับการทำงานและเวลา (Scheduling) ด้วย Apache Airflow
5.  **Containerization:** รันระบบทั้งหมดผ่าน Docker Compose

---

## 🛠️ Tech Stack

* **Language:** Python 3.9+
* **Orchestration:** Apache Airflow
* **Containerization:** Docker & Docker Compose
* **Data Processing:** Pandas / NumPy
* **Database:** PostgreSQL 15
* **Environment Management:** Python Dotenv

---

## 📂 โครงสร้างโปรเจกต์ (Project Structure)
```TEXT
├── dags/               # แฟ้มเก็บไฟล์ DAGs ของ Airflow
│   └── salemart_etl.py # ไฟล์กำหนด Workflow ของ ETL
├── docker-compose.yaml # ไฟล์สำหรับรัน Airflow และ DB ผ่าน Docker
├── Dockerfile          # สำหรับสร้าง Custom Airflow Image (ถ้ามี)
├── plugins/            # ส่วนเสริมของ Airflow (Custom Operators/Hooks)
├── scripts/            # Python scripts หลักสำหรับการ Transform
├── sql/                # SQL สำหรับการ Create Table / Schema
├── .env                # ไฟล์สำหรับเก็บ Database Credentials
└── requirements.txt    # รายการ Library
```

⚙️ วิธีการติดตั้งและรันโปรเจกต์ (Setup & Run)
1. เตรียมระบบ (Prerequisites)
  เครื่องของคุณต้องติดตั้ง:
    Docker Desktop
    Docker Compose

2. Clone Repository
  Bash
    git clone [https://github.com/ap1911ak/sale-mart-pipeline.git](https://github.com/ap1911ak/sale-mart-pipeline.git)
    cd sale-mart-pipeline
3. ตั้งค่า Environment Variables
  สร้างไฟล์ .env เพื่อกำหนดค่าพื้นฐานให้ Airflow และ PostgreSQL:
  Bash
    cp .env.example .env
4. เริ่มรันระบบด้วย Docker Compose
  คำสั่งนี้จะทำการ Pull Image ของ Airflow, Postgres และ Redis มาติดตั้งให้อัตโนมัติ:
  Bash
    docker-compose up -d
5. เข้าใช้งาน Airflow UI
  เมื่อระบบรันเสร็จเรียบร้อย คุณสามารถเข้าตรวจสอบ Pipeline ได้ที่:
  URL: http://localhost:8080
  Username: airflow (ตามที่ตั้งไว้ใน docker-compose)
  Password: airflow

🚀 การใช้งาน Pipeline (Usage)
  เข้าสู่ Airflow UI
  เปิดใช้งาน DAG ที่ชื่อ salemart_etl_pipeline
  คลิกที่ปุ่ม Trigger DAG เพื่อเริ่มทำงานทันที
  ตรวจสอบ Log ในแต่ละ Task (Extract -> Transform -> Load) เพื่อดูสถานะการทำงาน
