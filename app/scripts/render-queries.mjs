// Renders config/query-templates/*.sql into config/queries/ (the folder the
// analytics plugin reads), substituting the Unity Catalog location from env:
//   {{catalog}}       <- ALGAL_CATALOG
//   {{schema_prefix}} <- ALGAL_SCHEMA_PREFIX (default "water")
// The bundle sets both on the app, so the same source works in any workspace.
// Runs before `start` (on Databricks Apps) and before `dev` / `typegen` (locally,
// reading .env if present).
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const envFile = join(root, '.env');
if (existsSync(envFile)) process.loadEnvFile(envFile);

const catalog = process.env.ALGAL_CATALOG;
const schemaPrefix = process.env.ALGAL_SCHEMA_PREFIX || 'water';
if (!catalog) {
  console.error('render-queries: ALGAL_CATALOG is not set (set it in app config or .env).');
  process.exit(1);
}
const identifier = /^[A-Za-z0-9_]+$/;
if (!identifier.test(catalog) || !identifier.test(schemaPrefix)) {
  console.error(`render-queries: invalid catalog/schema prefix: ${catalog} / ${schemaPrefix}`);
  process.exit(1);
}

const src = join(root, 'config', 'query-templates');
const out = join(root, 'config', 'queries');
mkdirSync(out, { recursive: true });
const files = readdirSync(src).filter((f) => f.endsWith('.sql'));
for (const file of files) {
  const sql = readFileSync(join(src, file), 'utf8')
    .replaceAll('{{catalog}}', catalog)
    .replaceAll('{{schema_prefix}}', schemaPrefix);
  writeFileSync(join(out, file), sql);
}
console.log(`render-queries: ${files.length} queries -> ${catalog}.${schemaPrefix}_*`);
