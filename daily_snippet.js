const axios = require('axios');
const fs = require('fs');
const { exec } = require('child_process');

async function downloadDSEPDF() {
    const url = 'https://www.dse.com.bd/api/pdf-stream?s=aHR0cHM6Ly9vbGQuZHNlYmQub3JnL2Fzc2V0cy9wZGYvZHNlLWRhaWx5LW1hcmtldC1zbmlwcGV0LnBkZg%3D%3D';

    try {
        const response = await axios.get(url, {
            responseType: 'arraybuffer',
            headers: {
                'User-Agent': 'Mozilla/5.0',
                'Referer': 'https://www.dse.com.bd/'
            }
        });

        const contentType = response.headers['content-type'];
        const buffer = Buffer.from(response.data);

        console.log('Status:', response.status);
        console.log('Content-Type:', contentType);

        // Validate actual PDF content
        if (buffer.subarray(0, 5).toString() !== '%PDF-') {
            console.log(buffer.toString('utf8').substring(0, 1000));
            throw new Error('Response is not a valid PDF');
        }

        fs.writeFileSync(`C:\\Users\\ASUS\\Desktop\\merged_eod\\today/`+`dse-daily-market-snippet.pdf`, buffer);

        console.log('PDF downloaded successfully!');
        exec(`start "" "C:\\Users\\ASUS\\Desktop\\merged_eod\\today"`);
    } catch (error) {
        console.error('Download failed:', error.message);
    }
}

downloadDSEPDF();