const fs = require('fs');
const es = require('event-stream');
const path = require('path');

const tempDirectory = './tmp';
const outputDirectory = './output';

async function cleanTempFiles(directory) {
    try {
        const files = await fs.promises.readdir(directory);
        const promises = files.map(file => fs.promises.unlink(path.join(directory, file)));

        return Promise.all(promises);
    } catch (error) {
        return Promise.reject(error);
    }
}

function createNameTempfiles() {
    return new Promise((resolve, reject) => {
        let lineCount = 0;
        fs.createReadStream('./static/ids_names_file')
            .pipe(es.split())
            .pipe(
                es.mapSync(async (line) => {
                    try {
                        lineCount++;
                        const [id] = line.split(' , ');
                        const content = new Uint8Array(Buffer.from(line));
                        await fs.promises.writeFile(`./tmp/${id}`, content);
                    } catch (error) {
                        return console.error(`Error in "ids_names_file" operation on line: ${lineCount}, error: ${error}`);
                    }
                })
                    .on('error', function (err) {
                        reject(err);
                    })
                    .on('end', function () {
                        resolve('done');
                    })
            )
    });
}

function updateTempfiles() {
    return new Promise((resolve, reject) => {
        let lineCount = 0;
        fs.createReadStream('./static/ids_address_file')
            .pipe(es.split())
            .pipe(
                es.mapSync(async (line) => {
                    try {
                        lineCount++;
                        const [id, address] = line.split(' , ');
                        const stat = await fs.promises.stat(`./tmp/${id}`);
                        if (!stat.isFile()) {
                            return;
                        }

                        await fs.promises.appendFile(`./tmp/${id}`, ` , ${address}`);

                    } catch (error) {
                        if (error.code === 'ENOENT') {
                            error = 'ID not found';
                        }
                        return console.error(`Error in "ids_address_file" operation on line: ${lineCount}, error: ${error}`);
                    }
                })
                    .on('error', function (err) {
                        reject(err);
                    })
                    .on('end', function () {
                        resolve('done');
                    })
            )
    });
}

async function createMergedFile() {
    try {
        const outFilePath = `${outputDirectory}/ids_name_address_file`;
        await fs.promises.writeFile(outFilePath, new Uint8Array(Buffer.from('')));
        const files = await fs.promises.readdir(tempDirectory);
        const promises = files.map(file => {
            return new Promise(async (resolve, reject) => {
                try {
                    let content = await fs.promises.readFile(path.join(tempDirectory, file), 'utf-8');
                    content = new Uint8Array(Buffer.from(`${content}\n`));
                    resolve(fs.promises.appendFile(outFilePath, content));
                } catch (error) {
                    reject(error);
                }
            });
        });

        return Promise.all(promises);
    } catch (error) {
        return Promise.reject(error);
    }
}

async function main() {
    try {
        // clean output directory 
        await cleanTempFiles(outputDirectory);

        // create temp file for each id
        await createNameTempfiles();

        // add cities in respect to id using temp file 
        await updateTempfiles();

        // move all temp files contents to new file
        await createMergedFile();

        // clean temp file uses for storing id and names
        await cleanTempFiles(tempDirectory);
    } catch (error) {
        console.error(error);
    }
}
main();
