# LXtransport Production Deploy

## Target
- Domain: `https://spec.lxtransport.online/`
- Code path: `/opt/spec-extraction`
- Data path: `/var/lib/spec-extraction`
- Web bind: `127.0.0.1:8010`

## Steps
1. Upload the deployment zip to the server and unpack it into `/opt/spec-extraction`.
2. Create a virtual environment and install `requirements.txt`.
3. Copy `App/scripts/spec-extraction.env.example` to `/etc/spec-extraction.env` and fill in the real secrets.
4. Install the systemd units with `sudo bash App/scripts/install_systemd.sh`.
5. Copy `App/scripts/spec.lxtransport.online.nginx.conf` into Nginx sites-available and enable it.
6. Reload Nginx and start both systemd services.
7. Issue or attach a TLS certificate for `spec.lxtransport.online`.
8. Re-run the HTTPS health check after TLS is in place.

## Suggested Commands
```bash
sudo mkdir -p /opt/spec-extraction /var/lib/spec-extraction
sudo chown -R ubuntu:ubuntu /opt/spec-extraction /var/lib/spec-extraction
cd /opt/spec-extraction
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
sudo cp App/scripts/spec-extraction.env.example /etc/spec-extraction.env
sudo cp App/scripts/spec.lxtransport.online.nginx.conf /etc/nginx/sites-available/spec-extraction
sudo ln -s /etc/nginx/sites-available/spec-extraction /etc/nginx/sites-enabled/spec-extraction
sudo bash App/scripts/install_systemd.sh
sudo nginx -t
sudo systemctl restart spec-extraction-web spec-extraction-worker nginx
sudo certbot --nginx -d spec.lxtransport.online
curl -I https://spec.lxtransport.online/
```

## Notes
- Keep `/etc/spec-extraction.env` outside the repo and set real values for `SPEC_EXTRACTION_SECRET_KEY`, admin credentials, and `OPENAI_API_KEY`.
- `SPEC_EXTRACTION_HTTPS_ONLY=1` must stay enabled in production so session cookies are marked secure behind Nginx HTTPS.
- Keep `SPEC_EXTRACTION_MAX_UPLOAD_MB` in `/etc/spec-extraction.env` aligned with the Nginx `client_max_body_size` value in `spec.lxtransport.online.nginx.conf`.
- If `curl -I https://spec.lxtransport.online/` shows a certificate mismatch, finish the `certbot --nginx -d spec.lxtransport.online` step before testing login or uploads.

## Routine Update Workflow
Use production as the default target after confirmed implementation work.

Preferred local helper:
```powershell
$env:SPEC_EXTRACTION_DEPLOY_PASSWORD="..."
.\tools\deploy_online.ps1
```

Definition of done for confirmed changes:
1. Local verification passes.
2. The latest code is deployed to `/opt/spec-extraction`.
3. `spec-extraction-web.service` and `spec-extraction-worker.service` restart successfully.
4. `https://spec.lxtransport.online/api/health` returns OK.
5. If parsing changed, re-run the affected job online and verify the latest run uses the new build.
