# Dashboard

**Not initialised yet.** The dashboard is milestone M4, and the plan says `web/` is deliberately not
scaffolded before the milestone that needs it.

Locked stack at initialisation time: React + TypeScript, Vite, Tailwind CSS, shadcn/ui, Recharts.

When M4 starts, initialise here with `npm create vite@latest . -- --template react-ts`.

The API the dashboard will talk to is described in [../api/openapi.yaml](../api/openapi.yaml). Today
that document contains the collector and the query endpoints that exist in M1; the dashboard-specific
paths arrive with M4.
