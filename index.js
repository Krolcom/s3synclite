const AWS = require('@aws-sdk/client-s3')

const { stat, lstat, mkdir, unlink, rmdir, readFile, utimes } = require('fs').promises
const { existsSync } = require('fs')
const path = require('path')
const { dirname } = path
const { default: PQueue } = require('p-queue')
const getAllFiles = require('get-all-files').default

const removeDirIfEmpty = async path => {
  try {
    await rmdir(path)
  } catch (err) {
    return false
  }
  return true
}

const getFileUpdatedDate = async path => {
  try {
    return (await stat(path)).mtime
  } catch (e) {}
  // return epoch
  return new Date(628021800000)
}

/**
 * check to see if path is a directory
 * @param {String} path
 * @return {Promise<Boolean>}
 */
const isDirectory = async path => {
  try {
    return (await stat(path)).isDirectory()
  } catch (e) {}
  return false
}

const isSymlink = async path => {
  return (await lstat(path)).isSymbolicLink()
}

const deleteFolderFromBucket = async (source, region) => {
  // find path of s3 bucket
  const startOfPath = source.indexOf('/') + 1
  const Prefix = startOfPath === 0 ? '' : (source.substring(source.length - 1) === '/' ? source.substring(startOfPath) : source.substring(startOfPath) + '/')
  const Bucket = startOfPath === 0 ? source : source.split('/', 2)[0]

  let stillFindingFiles = true
  let ContinuationToken

  const s3 = new AWS.S3({
    apiVersion: '2006-03-01',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    },
    region
  })

  const deleteQueue = new PQueue({
    concurrency: 5,
    autoStart: true,
    intervalCap: 1,
    interval: 500
  })

  while (stillFindingFiles) {
    // find objects in s3 bucket
    const objects = await new Promise((resolve, reject) => {
      s3.listObjectsV2({
        Bucket,
        ContinuationToken,
        Prefix
      }, (err, data) => {
        if (err) return reject(err)
        resolve(data)
      })
    })

    // array of objects to be removed
    const Objects = []

    // loop through found objects
    for (const { Key } of objects.Contents || []) {
      // add item for removal
      Objects.push({ Key })
    }

    if (Objects.length) {
      console.log(`Deleting ${Objects.length} objects from ${Bucket}...`)

      // add the download to the queue
      deleteQueue.add(async () => {
        // make sure destination directory exists

        return s3.deleteObjects({
          Bucket,
          Delete: {
            Objects,
            Quiet: true
          }
        })
      })
    }

    stillFindingFiles = objects.IsTruncated
    ContinuationToken = objects.NextContinuationToken
  }

  await deleteQueue.onIdle()
}

exports.deleteFolderFromBucket = deleteFolderFromBucket

/**
 *
 *
 * @param {String} source s3 bucket and path
 * @param {String} destination local folder to download data
 * @param {String} region aws region
 * @param {boolean} [deleteRemoved=false] remove local objects that are no stored in s3
 * @returns {Promise<number>} files downloaded and deleted
 */
const downloadBucket = async (source, destination, region, deleteRemoved = false) => {
  await mkdir(destination, { recursive: true })

  // find path of s3 bucket
  const startOfPath = source.indexOf('/') + 1
  const Prefix = startOfPath === 0 ? '' : (source.substring(source.length - 1) === '/' ? source.substring(startOfPath) : source.substring(startOfPath) + '/')
  const Bucket = startOfPath === 0 ? source : source.split('/', 2)[0]

  const s3 = new AWS.S3({
    apiVersion: '2006-03-01',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    },
    region
  })

  const downloadQueue = new PQueue({
    concurrency: 20,
    autoStart: true,
    intervalCap: 2,
    interval: 100
  })

  const downloadedFiles = []

  // make destination directory incase it doesn't exist
  await mkdir(destination, { recursive: true })

  let stillFindingFiles = true
  let ContinuationToken

  while (stillFindingFiles) {
    // find objects in s3 bucket
    const objects = await new Promise((resolve, reject) => {
      s3.listObjectsV2({
        Bucket,
        ContinuationToken,
        Prefix
      }, (err, data) => {
        if (err) return reject(err)
        resolve(data)
      })
    })

    // loop through found objects
    for (const { Key, LastModified } of objects.Contents || []) {
      const localKey = Key.substring(Prefix.length)

      if (deleteRemoved) {
        downloadedFiles.push(localKey)
      }

      const outputFilePath = `${destination}/${localKey}`
      // check to see if the file is up to date
      const existingFileDate = await getFileUpdatedDate(outputFilePath)
      if (LastModified <= existingFileDate) {
        // console.debug(`${Key} is up to date.`)
        continue
      }

      if (existsSync(outputFilePath) && await isSymlink(outputFilePath)) {
        // console.log(`${Key} file is a symlink, skipping...`)
        continue
      }

      // add the download to the queue
      downloadQueue.add(async () => {
        // make sure destination directory exists
        await mkdir(dirname(outputFilePath), { recursive: true })

        var file = require('fs').createWriteStream(outputFilePath)

        /** @type {Object} */
        // @ts-ignore
        const data = (await s3.getObject({
          Bucket,
          Key
        })).Body

        const finishedDownloading = new Promise(function (resolve, reject) {
          data.on('end', () => resolve(data.read()))
          file.on('error', reject)
        })
        data.pipe(file)

        await finishedDownloading

        // set file times to the ones from s3
        await utimes(outputFilePath, LastModified, LastModified)

        console.log(`Downloaded: ${localKey}`)
        return data
      })
    }

    stillFindingFiles = objects.IsTruncated
    ContinuationToken = objects.NextContinuationToken
  }

  await downloadQueue.onIdle()

  let deletedFiles = 0
  if (deleteRemoved) {
    const existingFiles = (await getAllFiles.async.array(destination)).map(o => o.substring(destination.length + 1))
    // console.log(existingFiles)
    // console.log(downloadedFiles)
    const filesToDelete = existingFiles
      .filter(x => !downloadedFiles.includes(x))
      .concat(downloadedFiles.filter(x => !existingFiles.includes(x)))

    filesToDelete.sort((a, b) => b.length - a.length)

    for (const o of filesToDelete) {
      if ((await isDirectory(`${destination}/${o}`))) {
        if (await removeDirIfEmpty(`${destination}/${o}`)) {
          console.log(`Deleted Directory: ${destination}/${o} because it is empty.`)
        }
        continue
      }
      try {
        deletedFiles++
        await unlink(`${destination}/${o}`)
        console.log(`Deleted: ${o}`)
      } catch (e) {
        console.error(`Failed to delete: ${o}`)
      }
    }
  }
  return downloadedFiles.length + deletedFiles
}

exports.downloadBucket = downloadBucket

/**
 * Upload files from local to s3 bucket
 * @param {String} source path to upload
 * @param {String} destination destination s3 bucket and path
 * @param {String} region aws region ex: us-east-1
 * @param {boolean} [deleteRemoved=false] remove s3 objects that are no stored locally
 */
const uploadBucket = async (source, destination, region, deleteRemoved = false) => {
  // find path of s3 bucket
  const startOfPath = destination.indexOf('/') + 1
  const Prefix = startOfPath === 0 ? '' : (destination.substring(destination.length - 1) === '/' ? destination.substring(startOfPath) : destination.substring(startOfPath) + '/')
  const Bucket = startOfPath === 0 ? destination : destination.split('/', 2)[0]

  const s3 = new AWS.S3({
    apiVersion: '2006-03-01',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    },
    region
  })

  const uploadQueue = new PQueue({
    concurrency: 5,
    autoStart: true,
    intervalCap: 1,
    interval: 100
  })

  async function filter (arr, callback) {
    // eslint-disable-next-line symbol-description
    const fail = Symbol()
    return (await Promise.all(arr.map(async item => (await callback(item)) ? item : fail))).filter(i => i !== fail)
  }

  const localFiles = await filter((await getAllFiles.async.array(source)).map(o => o.substring(source.length + 1)), async o => {
    const filePath = `${source}/${o}`
    const result = await isSymlink(filePath)
    return !result
  })

  for (const relativeFilePath of localFiles) {
    const filePath = `${source}/${relativeFilePath}`

    // add the upload to the queue
    await uploadQueue.add(async () => {
      let uploadFile = false

      try {
        const objectMetadata = await s3.headObject({
          Bucket,
          Key: `${Prefix}${relativeFilePath}`
        })

        uploadFile = objectMetadata.LastModified < await getFileUpdatedDate(filePath)
      } catch (e) {
        uploadFile = true
      }

      if (!uploadFile) {
        return
      }

      const data = await s3.putObject({
        Body: await readFile(filePath),
        Bucket,
        Key: `${Prefix}${relativeFilePath}`
      })
      console.log(`Uploaded: ${relativeFilePath}`)
      return data
    })
  }

  // wait for uploads to finish
  await uploadQueue.onIdle()

  if (deleteRemoved) {
    const existingFiles = []
    let stillFindingFiles = true
    let ContinuationToken

    while (stillFindingFiles) {
      // find objects in s3 bucket
      const objects = await new Promise((resolve, reject) => {
        s3.listObjectsV2({
          Bucket,
          ContinuationToken,
          Prefix
        }, (err, data) => {
          if (err) return reject(err)
          resolve(data)
        })
      })

      // loop through found objects
      for (const { Key } of objects.Contents || []) {
        const localKey = Key.substring(Prefix.length)
        existingFiles.push(localKey)
      }

      stillFindingFiles = objects.IsTruncated
      ContinuationToken = objects.NextContinuationToken
    }

    const filesToDelete = existingFiles
      .filter(x => !localFiles.includes(x))
      .concat(localFiles.filter(x => !existingFiles.includes(x)))

    for (const o of filesToDelete) {
      await uploadQueue.add(async () => {
        await s3.deleteObject({
          Bucket,
          Key: `${Prefix}${o}`
        })
        console.log(`Deleted: ${o}`)
      })
    }
  }

  // wait for all downloads to finish
  await uploadQueue.onIdle()
}

exports.uploadBucket = uploadBucket
