import { readdir, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const sentinels = ['m4-synthetic-admin-token', 'm4-synthetic-viewer-password']
let count = 0
async function check(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name)
    if (entry.isDirectory()) await check(path)
    else {
      const body = await readFile(path)
      if (sentinels.some(value => body.includes(value))) throw new Error(`Synthetic credential leaked into ${path}`)
      if (entry.name.endsWith('.map')) throw new Error(`Production source map found: ${path}`)
      count++
    }
  }
}
await check(fileURLToPath(new URL('../dist', import.meta.url)))
console.log(`Checked ${count} production assets: no synthetic admin/viewer credentials or source maps.`)
