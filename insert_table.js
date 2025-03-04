require("dotenv").config(); // Load .env
const fs = require("fs");
const csv = require("csv-parser");
const mysql = require("mysql2");

// Ambil parameter dari command line (nama tabel)
const inputFile = process.argv[2]; // Nama file CSV
const tableName = process.argv[3]; // Nama Tabel
const jsonFile = "output.json"; // File JSON sementara

if (!inputFile) {
    console.error("❌ Harap masukkan nama file csv saat menjalankan skrip!");
    console.error("Contoh: node insert_table.js file.csv nama_tabel");
    process.exit(1);
}

if (!tableName) {
    console.error("❌ Harap masukkan nama tabel saat menjalankan skrip!");
    console.error("Contoh: node insert_table.js file.csv nama_tabel");
    process.exit(1);
}

let data = [];

// Fungsi untuk membersihkan header (huruf kecil, hapus simbol, ganti spasi dengan _)
const cleanHeader = (header) => {
    return header
        .toLowerCase()
        .replace(/[^a-z0-9 ]/g, "") // Hapus karakter selain huruf, angka, dan spasi
        .replace(/\s+/g, "_"); // Ganti spasi dengan _
};

// Proses membaca CSV dan mengubahnya ke JSON
fs.createReadStream(inputFile)
    .pipe(csv({ mapHeaders: ({ header }) => cleanHeader(header) }))
    .on("data", (row) => {
        data.push(row);
    })
    .on("end", () => {
        if (data.length === 0) {
            console.error("❌ CSV kosong atau format salah.");
            return;
        }

        fs.writeFileSync(jsonFile, JSON.stringify(data, null, 2));
        console.log(`✅ Konversi selesai! File JSON disimpan sebagai '${jsonFile}'`);

        // Setelah konversi selesai, eksekusi JSON ke database
        insertJsonToDatabase();
    });

// Fungsi untuk memasukkan JSON ke database
const insertJsonToDatabase = () => {
    // Konfigurasi koneksi database
    const connection = mysql.createConnection({
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME,
    });

    // Baca file JSON
    if (!fs.existsSync(jsonFile)) {
        console.error(`❌ File ${jsonFile} tidak ditemukan!`);
        process.exit(1);
    }

    const jsonData = JSON.parse(fs.readFileSync(jsonFile, "utf8"));

    if (jsonData.length === 0) {
        console.error("❌ Data JSON kosong.");
        process.exit(1);
    }

    // Ambil kolom dari JSON (gunakan object keys dari item pertama)
    const columns = Object.keys(jsonData[0]);

    // KOSONGKAN TABEL SEBELUM INSERT
    connection.query(`TRUNCATE TABLE \`${tableName}\``, (err) => {
        if (err) {
            console.error(`❌ Gagal mengosongkan tabel '${tableName}':`, err.message);
            connection.end();
            return;
        }
        console.log(`🗑️ Tabel '${tableName}' telah dikosongkan.`);

        // Buat query INSERT
        const insertQuery = `INSERT INTO \`${tableName}\` (${columns.map((col) => `\`${col}\``).join(", ")}) VALUES ?`;

        // Format data untuk VALUES
        const values = jsonData.map((row) => columns.map((col) => row[col] || null));

        connection.query(insertQuery, [values], (err, result) => {
            if (err) {
                console.error("❌ Gagal menjalankan SQL:", err.message);
                connection.end();
                return;
            }

            console.log(`✅ Berhasil memasukkan ${result.affectedRows} baris ke tabel '${tableName}'`);

            // Hapus file JSON setelah sukses
            fs.unlink(jsonFile, (err) => {
                if (err) {
                    console.error("⚠️ Gagal menghapus file JSON:", err.message);
                } else {
                    console.log(`🗑️ File JSON '${jsonFile}' telah dihapus.`);
                }
            });

            connection.end();
        });
    });
};
