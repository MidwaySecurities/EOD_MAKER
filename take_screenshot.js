const puppeteer = require('puppeteer');
const { exec } = require('child_process');

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

        // domcontentloaded + small settle wait is more reliable than
        // networkidle2 on sites with persistent polling/ads
        await page.goto('https://www.dsebd.org/', {
            waitUntil: 'domcontentloaded',
            timeout: 60000
        });
        await new Promise(r => setTimeout(r, 3000)); // let layout/images settle

        await page.screenshot({
            path: `${rootDir}/dse_index(${yymmdd}).png`,
            clip: { x: 380, y: 170, width: 580, height: 510 }
        });

        await page.close(); // free resources before opening the next page

        const page1 = await browser.newPage();
        await page1.setViewport({ width: 1920, height: 1080, deviceScaleFactor: 1 });
        await page1.setDefaultNavigationTimeout(60000);

        await page1.goto('https://www.dsebd.org/market-statistics.php', {
            waitUntil: 'domcontentloaded',
            timeout: 60000
        });

        await page1.waitForFunction(
            () => document.body.innerText.includes('Total number of scrips traded in Block'),
            { timeout: 30000 }
        );

        const dynamicHeight = await page1.evaluate(() => {
            const pres = Array.from(document.querySelectorAll('pre'));
            const target = pres.find(pre =>
                pre.innerText.includes('Total number of scrips traded in Block')
            );
            if (!target) return 550;
            const rect = target.getBoundingClientRect();
            return rect.bottom - 1420;
        });

        console.log('Calculated dynamic height:', dynamicHeight, ' Please wait...');

        await page1.screenshot({
            path: `${rootDir}/block_transaction(${yymmdd}).png`,
            clip: { x: 250, y: 1380, width: 590, height: dynamicHeight }
        });

        await page1.close();
    } catch (err) {
        console.error('Screenshot tool failed:', err);
        throw err;
    } finally {
        await browser.close();
    }

    exec(`start "" "${rootDir}"`);
};

module.exports = screenshotTool;
// screenshotTool()