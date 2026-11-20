// npm ships some packages' Go sources. Keep the dependency tree outside the
// parent module's ./... discovery; it is not QueueLens application source.
import { writeFile } from 'node:fs/promises'

await writeFile(new URL('../node_modules/go.mod', import.meta.url), 'module queuelens-dashboard-dependencies\n\ngo 1.26.0\n')
