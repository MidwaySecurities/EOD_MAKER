const fs = require('fs');
const puppeteer = require('puppeteer');
const { exec } = require('child_process');
const axios = require('axios');
const currentDate = new Date();
const currentMonthNum = currentDate.getMonth() + 1;
const currentDateNum = currentDate.getDate();
const currentYear = currentDate.getFullYear();

const yymmdd = `${currentYear}${String(currentMonthNum).padStart(2, '0')}${String(currentDateNum).padStart(2, '0')}`;
const rootDir = "C:/Users/ASUS/Desktop/merged_eod/today";

const screenshotTool = async () => {
    const browser = await puppeteer.launch({
        headless: true,
        protocolTimeout: 120000,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-gpu',
            '--disable-dev-shm-usage'
        ]
    });

    try {
        const page = await browser.newPage();
        await page.setViewport({ width: 1920, height: 1080, deviceScaleFactor: 1 });
        await page.setDefaultNavigationTimeout(60000);
        await page.goto('https://www.dsebd.org/', {
            waitUntil: 'domcontentloaded',
            timeout: 60000
        });
        await new Promise(r => setTimeout(r, 5000));

        await page.screenshot({
            path: `${rootDir}/dse_index(${yymmdd}).png`,
            clip: { x: 240, y: 550, width: 1450, height: 410 }
        });

        await page.close();
        const { data } = await axios.get('https://www.dse.com.bd/api/live/market-statistics',
            {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': 'https://www.dse.com.bd/',
                    'Origin': 'https://www.dse.com.bd',
                    'Connection': 'keep-alive'
                },
                timeout: 15000
            });
        const blockTradeData = await data['block']['rows'];

        const headers = ['Instr Code', 'Max Price', 'Min Price', 'Trades', 'Quantity', 'Value(In Mn)'];
        const jsonHeaders = ['code', 'maxPrice', 'minPrice', 'trades', 'quantity', 'valueMn'];
        const csvRows = [];

        csvRows.push(headers.join(','));

        for (const row of blockTradeData) {
            const values = jsonHeaders.map(header => {
                const val = row[header];
                const stringVal = typeof val === 'object' ? JSON.stringify(val) : String(val);
                return stringVal.includes(',') ? `"${stringVal.replace(/"/g, '""')}"` : stringVal;
            });
            csvRows.push(values.join(','));
        }

        const csvContent = csvRows.join('\n');

        fs.writeFile(`${rootDir}/block_transaction(${yymmdd}).csv`, csvContent, (err) => {
            if (err) {
                console.error('Error writing CSV file:', err);
            } else {
                console.log('CSV file successfully created using native fs!');
            }
        });

    } catch (err) {
        console.error('Screenshot tool failed:', err);
        throw err;
    } finally {
        await browser.close();
    }

    exec(`start "" "${rootDir}"`);
};

// screenshotTool()
module.exports = screenshotTool;
