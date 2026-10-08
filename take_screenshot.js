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
        const { data } = await axios.get('https://www.dse.com.bd/api/live/market-statistics');
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
