# TRAID — TSEW RAID Manager

A web-based storage management appliance for Debian that makes the most of
mismatched drives. TRAID uses a tier-slicing algorithm to pool drives of
different sizes into a single logical volume without wasting space, then
manages the whole system through a browser UI — including filesystems,
virtual machines, Docker containers, scheduled backups, and file sharing.

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
  ├─ mdadm        — array creation, reshape, replace, grow, shrink
  ├─ parted       — GPT partitioning
  ├─ lvm2         — PV / VG / LV management
  ├─ btrfs-progs  — subvolumes, snapshots, scrub, balance, send/receive
  ├─ smartctl     — SMART self-tests
  ├─ badblocks    — write-mode surface scan
  ├─ shred        — secure erase
  ├─ virsh        — KVM virtual machine lifecycle
  ├─ docker       — container and image management
  ├─ rsync/mount  — scheduled backup jobs
  └─ exportfs / smbd — NFS and Samba file sharing
```

The socket is `0660 root:www-data` so only the web process can write to it.
Every request is validated against a strict command whitelist before any
subprocess is spawned. No shell interpolation is used anywhere.

---

## Features

### Storage — Array management

- **Create** a TRAID-1 or TRAID-2 array from any combination of drives
- **Capacity preview** — see usable space and tier breakdown before committing
- **Migrate** between TRAID-1 and TRAID-2 online (reshape in place, power-fail
  safe with mdadm `--backup-file` on `/boot`)
- **Grow** — add a new drive to an existing array
- **Shrink** — remove a drive from an array
- **Replace** — hot-swap a failing or failed drive
- **Delete** — wipe all arrays and partition tables and return drives to blank

### Storage — Filesystem management

- **Format** volumes as ext4 or btrfs
- **Mount / unmount** with immutable mount-point enforcement
- **Btrfs subvolumes and snapshots** — create, delete, set default, list with
  snapshot detection via libbtrfsutil
- **Btrfs scrub** — start, pause, resume, cancel, and view last result
- **Btrfs balance** — with optional usage filters
- **Btrfs defrag** and **dedup** (duperemove)
- **Btrfs quotas** — enable quota groups, set per-qgroup limits
- **Btrfs send / receive** — snapshot streaming for off-appliance backups
- **Disk usage** — used/free display for ext4 and btrfs

### Storage — Data operations

- **Clone** — full block-level copy of the logical volume to a single disk
- **Volume backup** — rsync the filesystem to an NFS or CIFS/Samba network share

### Storage — Disk health

- **SMART short test** — quick drive self-test (~2 min)
- **SMART long test** — full drive self-test (drive-dependent, can be hours)
- **Badblocks** — destructive write-mode surface scan

### Storage — Secure erase

Three modes, implemented with `shred`:

| Mode | Passes | Use case |
|------|--------|---------|
| Quick Erase | 1 × zeros | Reuse in a trusted environment |
| DoD Short | 3 × random | General data destruction |
| DoD 5220.22-M | 7 × random + zero | Disposal or recycling |

### Virtual Machines

- List all KVM/QEMU virtual machines with state, vCPU, memory, and VNC port
- **Start, shutdown, force-off, suspend, resume** any VM
- **Create** a new VM from an ISO — name, RAM, vCPU count, and disk size
  configurable; ISOs stored in `/var/lib/traid/iso/`
- **Delete** a VM, optionally removing its disk image

### Containers

- List all Docker containers (running and stopped) with image, state, and ports
- **Start, stop, remove** any container
- **View live logs** (last 200 lines, configurable up to 10 000)
- List local Docker images with repository, tag, and size
- **Pull** an image from any registry as a background job
- **Remove** images, with optional force flag
- **System prune** — remove all stopped containers, unused images, and networks

### Scheduled Backups

- Define backup jobs with a name, source volume group, destination, and interval
- Supported destination protocols: **rsync (local)**, **NFS**, **CIFS/Samba**,
  **btrfs send** (stream to file)
- **Run now** triggers an immediate backup as a background job
- Per-job history: last 20 runs with timestamp, status, and duration
- Background scheduler fires due jobs every 60 seconds

### File Sharing

- **NFS exports** — add and remove entries in `/etc/exports` with per-export
  client specs and options; reloads `exportfs` automatically
- **Samba shares** — add and remove sections in `/etc/samba/smb.conf`; reloads
  `smbd` automatically; configurable public/guest and writable flags
- All share paths are restricted to `/srv/traid/`, `/mnt/traid/`, and
  `/var/lib/traid/` — path traversal is blocked at the API and worker layers

### Task management

- All long-running operations run as background jobs with live progress
- SMART, badblocks, erase, VM create, image pull, and backup jobs can be
  **cancelled** at any time
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
`btrfs-progs` `python3-btrfsutil` `duperemove` `smartmontools` `rsync`
`cifs-utils` `nfs-common` `nfs-kernel-server` `samba` `libvirt-daemon-system`
`libvirt-clients` `virtinst` `qemu-system-x86` `docker.io` `lighttpd`

---

## Project layout

```
├── api/
│   ├── main.py              FastAPI application, all HTTP and WebSocket routes
│   ├── models.py            Pydantic v2 request/response schemas
│   ├── uds_client.py        Async Unix Domain Socket client
│   ├── websocket_manager.py WebSocket pool + mdstat live-stream
│   ├── vm_routes.py         KVM virtual machine endpoints
│   ├── docker_routes.py     Container and image endpoints
│   ├── backup_routes.py     Scheduled backup job endpoints
│   └── sharing_routes.py    NFS and Samba sharing endpoints
├── worker/
│   ├── daemon.py            Root daemon — UDS server, job scheduler, lock
│   ├── command_validator.py Security kernel — request whitelist enforcement
│   ├── disk_ops.py          Subprocess wrappers (mdadm, parted, lvm, shred…)
│   ├── fs_ops.py            Filesystem operations (format, mount, btrfs…)
│   ├── vm_ops.py            KVM/virsh operations
│   ├── docker_ops.py        Docker CLI operations
│   ├── backup_scheduler.py  Scheduled backup jobs and tick loop
│   ├── sharing_ops.py       NFS exports and Samba share management
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
├── tests/                   Unit and integration tests (748 tests)
└── LICENSE                  MIT
```

---

## Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# All tests (748, no hardware required for most)
pytest tests/

# Targeted suites
pytest tests/test_traid_algorithm.py    # tier-slicing algorithm
pytest tests/test_command_validator.py  # security whitelist
pytest tests/test_sharing_ops.py        # NFS/Samba file manipulation
pytest tests/test_backup_scheduler.py   # backup job CRUD and scheduling
pytest tests/test_docker_ops.py         # Docker CLI wrappers (mocked)
pytest tests/test_vm_ops.py             # KVM/virsh wrappers (mocked)
pytest tests/test_api_routes.py         # full API stack (mocked worker)
```

The tier-slicing algorithm (`worker/traid_algorithm.py`) and all new worker
modules are pure Python with all subprocess calls mocked — the full test suite
runs without any drives, VMs, Docker daemon, or network shares.

---

## Security notes

- The worker daemon validates every incoming request against a strict whitelist
  (`worker/command_validator.py`) before touching any subprocess.
- All subprocess calls use `asyncio.create_subprocess_exec` — no shell
  interpolation is possible.
- Device path inputs are validated against a strict regex before use; dangerous
  pseudo-devices (`/dev/null`, `/dev/zero`, etc.) are explicitly blocked.
- Share paths (NFS, Samba, backup destinations) are restricted to
  `/srv/traid/`, `/mnt/traid/`, and `/var/lib/traid/`; `..` path components
  are rejected by a negative lookahead in the validation regex.
- Report filenames served by the API are validated against a UUID + suffix
  pattern to prevent path traversal.
- The Unix Domain Socket is `0660 root:www-data`; only the web process can
  reach the daemon.
- VM and Docker operations run as root inside the worker but all inputs
  (VM names, ISO filenames, image tags, container IDs) are validated against
  strict allow-list regexes before being passed to any subprocess.

---

## License

MIT — see [LICENSE](LICENSE).
