# TRAID — TSEW RAID Manager

A web-based storage management appliance for Debian that makes the most of
mismatched drives. TRAID uses a tier-slicing algorithm to pool drives of
different sizes into a single logical volume without wasting space, then
manages it through a browser UI.

---

## The TRAID algorithm

Standard RAID wastes capacity when drives are different sizes — a 4 TB drive
paired with a 2 TB drive in RAID 1 leaves 2 TB unused. TRAID avoids this by
slicing each drive into tiers:

1. Sort all drives by size (smallest first).
2. Slice every drive at the size of the smallest, form a RAID group from
   those equal-sized slices, and record the usable capacity.
3. Remove the smallest drive from the pool and repeat with the remainder.
4. Continue until too few drives remain for the chosen redundancy level.
5. Join all RAID groups into a single LVM logical volume.

**Example — 3 × 4 TB + 1 × 2 TB drives (TRAID-1):**

| Tier | Drives | Slice size | RAID level | Usable |
|------|--------|-----------|------------|--------|
| 0 | 4 | 2 TB each | RAID 5 | 6 TB |
| 1 | 3 | 2 TB each | RAID 5 | 4 TB |
| **Total** | | | | **10 TB** |

A naive RAID 5 across the same four drives at minimum size would yield 6 TB.
TRAID recovers the extra 4 TB from the larger drives.

### Redundancy modes

| Mode | Fault tolerance | RAID levels used |
|------|----------------|-----------------|
| **TRAID-1** | 1 drive | RAID 5 (3+ drives per tier), RAID 1 (2 drives) |
| **TRAID-2** | 2 drives | RAID 6 (4+ drives per tier) |

---

## Architecture

TRAID uses a split-privilege design. The web process runs unprivileged
(`www-data`) and never calls system tools directly. All privileged operations
go through a Unix Domain Socket to a root daemon.

```
Browser
  │  HTTP / WebSocket
  ▼
lighttpd  (port 80, proxies /api/ and /ws/)
  │
  ▼
uvicorn / FastAPI  (127.0.0.1:8000, runs as www-data)
  │  Unix Domain Socket  /run/traid.sock
  ▼
worker daemon  (runs as root)
  ├─ mdadm   — array creation, reshape, replace, grow, shrink
  ├─ parted  — GPT partitioning
  ├─ lvm2    — PV / VG / LV management
  ├─ smartctl — SMART self-tests
  ├─ badblocks — write-mode surface scan
  └─ shred   — secure erase
```

The socket is `0660 root:www-data` so only the web process can write to it.
Every request is validated against a strict command whitelist before any
subprocess is spawned. No shell interpolation is used anywhere.

---

## Features

### Array management
- **Create** a TRAID-1 or TRAID-2 array from any combination of drives
- **Capacity preview** — see usable space and tier breakdown before committing
- **Migrate** between TRAID-1 and TRAID-2 online (reshape in place, power-fail
  safe with mdadm `--backup-file` on `/boot`)
- **Grow** — add a new drive to an existing array
- **Shrink** — remove a drive from an array
- **Replace** — hot-swap a failing or failed drive
- **Delete** — wipe all arrays and partition tables and return drives to blank

### Data operations
- **Clone** — full block-level copy of the logical volume to a single disk
- **Backup** — rsync the filesystem to an NFS or CIFS/Samba network share

### Disk health
- **SMART short test** — quick drive self-test (~2 min)
- **SMART long test** — full drive self-test (drive-dependent, can be hours)
- **Badblocks** — destructive write-mode surface scan

### Secure erase
Three modes, implemented with `shred`:

| Mode | Passes | Use case |
|------|--------|---------|
| Quick Erase | 1 × zeros | Reuse in a trusted environment |
| DoD Short | 3 × random | General data destruction |
| DoD 5220.22-M | 7 × random + zero | Disposal or recycling |

### Task management
- All long-running operations run as background jobs with live progress
- SMART, badblocks, and erase jobs can be **cancelled** at any time
- One destructive array operation runs at a time (global lock); concurrent
  requests receive an `ARRAY_BUSY` response
- SMART tests run in parallel and are not blocked by the lock
- Job reports (SMART, badblocks, erase) are viewable inline or downloadable

---

## Installation

TRAID is packaged as a Debian `.deb`. The package installs all runtime
dependencies automatically.

```bash
sudo dpkg -i traid_*.deb
sudo apt-get install -f   # resolve any missing dependencies
```

The installer:
1. Installs Python 3.13, the application, and a pre-built virtualenv under
   `/usr/lib/traid/`
2. Installs and enables two systemd services: `traid-worker` (root) and
   `traid-web` (www-data)
3. Configures lighttpd to proxy `/api/` and `/ws/` to uvicorn

Once installed, open `http://<appliance-ip>/` in a browser.

### Runtime dependencies

Declared in `debian/control` and installed automatically:

`python3.13` `python3.13-venv` `mdadm` `lvm2` `parted` `e2fsprogs`
`xfsprogs` `smartmontools` `rsync` `cifs-utils` `nfs-common` `lighttpd`

---

## Project layout

```
├── api/
│   ├── main.py              FastAPI application, all HTTP and WebSocket routes
│   ├── models.py            Pydantic v2 request/response schemas
│   ├── uds_client.py        Async Unix Domain Socket client
│   └── websocket_manager.py WebSocket pool + mdstat live-stream
├── worker/
│   ├── daemon.py            Root daemon — UDS server, job scheduler, lock
│   ├── command_validator.py Security kernel — request whitelist enforcement
│   ├── disk_ops.py          Subprocess wrappers (mdadm, parted, lvm, shred…)
│   ├── traid_algorithm.py   Pure-Python tier-slicing algorithm (no I/O)
│   └── mdstat_reader.py     /proc/mdstat poller with asyncio queue fan-out
├── shared/
│   └── protocol.py          UDS message framing (4-byte length prefix + JSON)
├── static/
│   └── index.html           Single-file Alpine.js + Tailwind CSS SPA
├── systemd/
│   ├── traid-worker.service
│   └── traid-web.service
├── debian/                  Debian packaging
└── tests/                   Unit and integration tests
```

---

## Development

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Unit tests (no hardware required)
pytest tests/test_traid_algorithm.py
pytest tests/test_command_validator.py
pytest tests/test_mdstat_reader.py
pytest tests/test_uds_protocol.py

# API tests (mocked worker)
pytest tests/test_api_routes.py
```

The tier-slicing algorithm (`worker/traid_algorithm.py`) is pure Python with
no subprocess calls and can be tested completely without drives.

---

## Security notes

- The worker daemon validates every incoming request against a strict whitelist
  (`worker/command_validator.py`) before touching any subprocess.
- All subprocess calls use `asyncio.create_subprocess_exec` — no shell
  interpolation is possible.
- Device path inputs are validated against a strict regex
  (`^/dev/[a-z]{2,8}[0-9]{0,3}(p[0-9]{1,3})?$`) before use.
- Report filenames served by the API are validated against a UUID + suffix
  pattern to prevent path traversal.
- The Unix Domain Socket is `0660 root:www-data`; only the web process can
  reach the daemon.
