require("dotenv").config(); // Load .env
const fs = require("fs");
const csv = require("csv-parser");
const mysql = require("mysql2");

const outputFile = "create_table.sql";

// Ambil nama tabel dari argument CLI
const tableName = process.argv[3];
const inputFile = process.argv[2]

if (!inputFile) {
    console.error("❌ Harap masukkan nama file csv saat menjalankan skrip!");
    console.error("Contoh: node create_table.js file.csv nama_tabel");
    process.exit(1);
}

if (!tableName) {
    console.error("❌ Harap masukkan nama tabel saat menjalankan skrip!");
    console.error("Contoh: node create_table.js file.csv nama_tabel");
    process.exit(1);
}


let columns = [];
let rows = [];

// Fungsi untuk membersihkan header (huruf kecil, hapus simbol, ganti spasi dengan _)
const cleanHeader = (header) => {
    return header
        .toLowerCase() // Ubah jadi huruf kecil
        .replace(/[^a-z0-9 ]/g, "") // Hapus karakter selain huruf, angka, dan spasi
        .replace(/\s+/g, "_"); // Ganti spasi dengan _
};

fs.createReadStream(inputFile)
    .pipe(csv())
    .on("headers", (headers) => {
        columns = headers.map(cleanHeader); // Bersihkan header
    })
    .on("data", (row) => {
        const values = Object.values(row).map((val) => `'${val.replace(/'/g, "''")}'`);
        rows.push(`(${values.join(", ")})`);
    })
    .on("end", () => {
        if (columns.length === 0 || rows.length === 0) {
            console.error("CSV kosong atau format salah.");
            return;
        }

        // Buat perintah CREATE TABLE dengan DROP TABLE IF EXISTS
        const createTableQuery =
            `DROP TABLE IF EXISTS \`${tableName}\`;\n` + // Hapus tabel jika sudah ada
            `CREATE TABLE \`${tableName}\` (\n` +
            columns.map((col) => `  \`${col}\` VARCHAR(255)`).join(",\n") +
            `\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n\n`;

        // Simpan ke file SQL
        fs.writeFileSync(outputFile, createTableQuery);
        console.log(`✅ Struktur tabel berhasil dibuat!`);

        console.log(`Mengeksekusi SQL ke database...`);
        executeSQL(outputFile);
    });

/**
* Fungsi untuk menjalankan SQL di MySQL
*/
function executeSQL(sql) {
    const dbConfig = {
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME,
        multipleStatements: true, // Penting untuk menjalankan beberapa query sekaligus
    };

    // Koneksi ke database
    const connection = mysql.createConnection(dbConfig);
    connection.connect((err) => {
        if (err) {
            console.error("❌ Koneksi ke database gagal:", err);
            return;
        }
        console.log("✅ Terhubung ke database!");

        // Baca isi file SQL
        fs.readFile(sql, "utf8", (err, sql) => {
            if (err) {
                console.error("❌ Gagal membaca file SQL:", err);
                return;
            }

            // Jalankan query dari file SQL
            connection.query(sql, (err, results) => {
                if (err) {
                    console.error("❌ Gagal menjalankan SQL:", err);
                } else {
                    console.log("✅ Struktur tabel berhasil dieksekusi di database!");
                    // Hapus file SQL setelah berhasil dieksekusi
                    fs.unlinkSync(outputFile);
                }

                // Tutup koneksi
                connection.end();
            });
        });
    });
}
