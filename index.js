const fs = require('fs');
const csvParser = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;


const currentDate = new Date();
const currentMonthNum = new Date().getMonth()
const currentDateNum = new Date().getDate()
const currentYear = currentDate.getFullYear();
const yymmdd = `${currentYear}${currentMonthNum.toString().length < 2 ? '0' + (currentMonthNum + 1).toString() : (currentMonthNum + 1).toString()}${currentDateNum.toString().length < 2 ? '0' + currentDateNum.toString() : currentDateNum.toString()}`


const dir_dse_merged_eod = `C:\\Users/ASUS/Desktop/merged_eod/merged-eod(20250811_${yymmdd}).csv`;
const dir_amar_stock_merged_eod = `C:\\Users/ASUS/Desktop/merged_eod/merged-amar_stock_eod(20250811_${yymmdd}).csv`;
const output_file = `C:\\Users/ASUS/Desktop/merged_eod/today/final-merged-eod(20250811_${yymmdd}).csv`;
const dse_dsex_file = 'C:\\Users/ASUS/Downloads/dsex.csv';
const modified_dir = `E:\\amar_stock_modified`;



const modified_files = fs.readdirSync(modified_dir);
function readCsv(file) {
    return new Promise((resolve, reject) => {
        const rows = [];
        fs.createReadStream(file)
            .pipe(csvParser())
            .on('data', row => rows.push(row))
            .on('end', () => resolve(rows))
            .on('error', reject);
    });
}

(async () => {
    try {
        const results = await readCsv(dir_dse_merged_eod);
        const result_amar_stock = await readCsv(dir_amar_stock_merged_eod);
        const result_dsex = await readCsv(dse_dsex_file);

        const modified_result_of_dse = results.map(item => ({
            ...item,
            identifier: `${item.TradeDate}${item.Open}${item.High}${item.Low}${item.Close}`,
            Volume: null
        })).filter((item) => {
            return item.SecurityCode !== 'DS30' && item.SecurityCode !== 'DSES';
        });

        const amarStockLookup = {};
        result_amar_stock.forEach(item => {
            const identifier = `${item.Date}${item.Open}${item.High}${item.Low}${item.Close}`;
            amarStockLookup[identifier] = item.Volume;
        });

        const modify_dsex = result_dsex.map(item => {
            let parts = item.Date.includes('-') ? item.Date.split('-') : item.Date.split('/');
            if (parts[0].length === 4) {
                return { ...item, Date: parts.join('') };
            }
            const date = `${parts[2]}${parts[1].padStart(2, '0')}${parts[0].padStart(2, '0')}`;
            return { ...item, Date: date };
        });

        const dsex_lookup = {};
        modify_dsex.forEach(item => {
            dsex_lookup[`${item.Date}-DSEX_DATE`] = item.Date;
            dsex_lookup[`${item.Date}-DSEX_TOTAL_TRADE`] = item['Total Trade'];
            dsex_lookup[`${item.Date}-DSEX_TOTAL_VOLUME`] = item['Total Volume'];
            dsex_lookup[`${item.Date}-DSEX_TOTAL_VALUE`] = item['Total Value in Taka (mn)'];
        });

        const modifiedFilesLookUp = {};
        modified_files.forEach(f => {
            modifiedFilesLookUp[f.split('_')[0]] = f;
        });

        const mergedResult = modified_result_of_dse
            .filter(item => item.SecurityCode !== 'DS30' && item.SecurityCode !== 'DSES')
            .map(item => {
                if (amarStockLookup[item.identifier]) {
                    return { ...item, Volume: amarStockLookup[item.identifier] };
                }
                if (item.TradeDate === dsex_lookup[`${item.TradeDate}-DSEX_DATE`] && item.SecurityCode === 'DSEX') {
                    return {
                        ...item,
                        Volume: dsex_lookup[`${item.TradeDate}-DSEX_TOTAL_VOLUME`],
                        ['Total Value in Taka (mn)']: dsex_lookup[`${item.TradeDate}-DSEX_TOTAL_VALUE`],
                        ['Total Trade']: dsex_lookup[`${item.TradeDate}-DSEX_TOTAL_TRADE`],
                    };
                }
                return item;
            });

        const tempObj = {};
        for (const row of mergedResult) {
            if (modifiedFilesLookUp[row.TradeDate]) {
                tempObj[row.TradeDate] = await readCsv(`${modified_dir}/${modifiedFilesLookUp[row.TradeDate]}`);
            }
        }
        const finalMergedData = mergedResult.filter(item => {
            if (item.SecurityCode.includes('.AT')) {
                return false;
            }else {
                return true;
            }
        }).map(item => {
            if (tempObj[item.TradeDate]) {
                const foundEle = tempObj[item.TradeDate].find(ele =>
                    `${ele['TRADING CODE']}` === `${item.SecurityCode.includes('.SC') ? item.SecurityCode.replace('.SC', '') : item.SecurityCode}`
                );

                if (foundEle) {
                    return {
                        ...item,
                        ['Total Trade']: foundEle.Trade,
                        ['Total Value in Taka (mn)']: foundEle['VALUE(mn)'],
                        ['Volume']: foundEle['Volume']
                    };
                }
            }
            return item;
        });

        const csvWriter = createCsvWriter({
            path: output_file,
            header: [
                { id: 'SecurityCode', title: '_SecurityCode' },
                { id: 'ISIN', title: '_ISIN' },
                { id: 'AssetClass', title: '_AssetClass' },
                { id: 'CompulsorySpot', title: '_CompulsorySpot' },
                { id: 'TradeDate', title: '_TradeDate' },
                { id: 'Close', title: '_Close' },
                { id: 'Open', title: '_Open' },
                { id: 'High', title: '_High' },
                { id: 'Low', title: '_Low' },
                { id: 'Var', title: '_Var' },
                { id: 'VarPercent', title: '_VarPercent' },
                { id: 'Volume', title: '_Volume' },
                { id: 'Total Value in Taka (mn)', title: 'Total Value in Taka (mn)' },
                { id: 'Total Trade', title: 'Total Trade' }
            ]
        });

        await csvWriter.writeRecords(finalMergedData);
        console.log(`CSV file created successfully at: ${output_file}`);

    } catch (err) {
        console.error("Error:", err);
    }
})();
