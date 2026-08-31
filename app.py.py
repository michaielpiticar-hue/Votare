import http.server
import socketserver
import sqlite3
import os
import urllib.parse
import secrets
import html
import threading
from datetime import datetime

PORT = int(os.environ.get("PORT", "5000"))
DATABASE = os.environ.get("DATABASE", "votare.db")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")


class RowProxy:
    def __init__(self, values, columns):
        self._values = tuple(values)
        self._map = {columns[i]: self._values[i] for i in range(len(columns))}

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return self._map[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)


class CursorProxy:
    def __init__(self, cursor, lastrowid=None):
        self._cursor = cursor
        self.lastrowid = lastrowid if lastrowid is not None else getattr(cursor, "lastrowid", None)

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def _columns(self):
        return [d.name if hasattr(d, "name") else d[0] for d in (self._cursor.description or [])]

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        return RowProxy(row, self._columns())

    def fetchall(self):
        rows = self._cursor.fetchall()
        columns = self._columns()
        return [RowProxy(row, columns) for row in rows]


class PostgresConnection:
    def __init__(self, conn):
        self._conn = conn

    @staticmethod
    def _sql(sql):
        sql = sql.replace("BEGIN IMMEDIATE", "BEGIN")
        sql = sql.replace("c.name COLLATE NOCASE", "LOWER(c.name)")
        sql = sql.replace("name COLLATE NOCASE", "LOWER(name)")
        return sql.replace("?", "%s")

    def execute(self, sql, params=()):
        cur = self._conn.cursor()
        sql2 = self._sql(sql)
        lastrowid = None
        stripped = sql2.lstrip().upper()
        if stripped.startswith("INSERT INTO") and " RETURNING " not in stripped:
            sql2 = sql2.rstrip().rstrip(";") + " RETURNING id"
            cur.execute(sql2, params)
            row = cur.fetchone()
            if row:
                lastrowid = row[0]
        else:
            cur.execute(sql2, params)
        return CursorProxy(cur, lastrowid)

    def cursor(self):
        return PostgresCursor(self)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


class PostgresCursor:
    def __init__(self, connection):
        self._connection = connection
        self._proxy = None

    def execute(self, sql, params=()):
        self._proxy = self._connection.execute(sql, params)
        return self

    @property
    def lastrowid(self):
        return self._proxy.lastrowid if self._proxy else None

    @property
    def rowcount(self):
        return self._proxy.rowcount if self._proxy else -1

    def fetchone(self):
        return self._proxy.fetchone()

    def fetchall(self):
        return self._proxy.fetchall()


def get_db():
    if DATABASE_URL:
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "DATABASE_URL este setat, dar lipsește psycopg. Instalează: pip install psycopg[binary]"
            ) from exc
        conn = psycopg.connect(DATABASE_URL)
        return PostgresConnection(conn)

    conn = sqlite3.connect(DATABASE, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    conn = get_db()
    if DATABASE_URL:
        statements = [
            """CREATE TABLE IF NOT EXISTS polls (
                id BIGSERIAL PRIMARY KEY, public_id TEXT UNIQUE NOT NULL, title TEXT NOT NULL,
                is_open INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS categories (
                id BIGSERIAL PRIMARY KEY, poll_id BIGINT NOT NULL, name TEXT NOT NULL,
                max_choices INTEGER NOT NULL, sort_order INTEGER NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS candidates (
                id BIGSERIAL PRIMARY KEY, poll_id BIGINT NOT NULL, name TEXT NOT NULL,
                sort_order INTEGER NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS vote_codes (
                id BIGSERIAL PRIMARY KEY, poll_id BIGINT NOT NULL, code TEXT UNIQUE NOT NULL,
                used INTEGER NOT NULL DEFAULT 0, used_at TEXT)""",
            """CREATE TABLE IF NOT EXISTS ballots (
                id BIGSERIAL PRIMARY KEY, poll_id BIGINT NOT NULL, submitted_at TEXT NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS selections (
                id BIGSERIAL PRIMARY KEY, ballot_id BIGINT NOT NULL, category_id BIGINT NOT NULL,
                candidate_id BIGINT NOT NULL)""",
            "CREATE INDEX IF NOT EXISTS idx_categories_poll ON categories(poll_id)",
            "CREATE INDEX IF NOT EXISTS idx_candidates_poll ON candidates(poll_id)",
            "CREATE INDEX IF NOT EXISTS idx_vote_codes_poll ON vote_codes(poll_id)",
            "CREATE INDEX IF NOT EXISTS idx_ballots_poll ON ballots(poll_id)",
            "CREATE INDEX IF NOT EXISTS idx_selections_ballot ON selections(ballot_id)",
            "CREATE INDEX IF NOT EXISTS idx_selections_category_candidate ON selections(category_id, candidate_id)",
        ]
        for stmt in statements:
            conn.execute(stmt)
    else:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS polls (
                id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT UNIQUE NOT NULL, title TEXT NOT NULL,
                is_open INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT, poll_id INTEGER NOT NULL, name TEXT NOT NULL,
                max_choices INTEGER NOT NULL, sort_order INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT, poll_id INTEGER NOT NULL, name TEXT NOT NULL,
                sort_order INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS vote_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, poll_id INTEGER NOT NULL, code TEXT UNIQUE NOT NULL,
                used INTEGER NOT NULL DEFAULT 0, used_at TEXT);
            CREATE TABLE IF NOT EXISTS ballots (
                id INTEGER PRIMARY KEY AUTOINCREMENT, poll_id INTEGER NOT NULL, submitted_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS selections (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ballot_id INTEGER NOT NULL, category_id INTEGER NOT NULL,
                candidate_id INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_categories_poll ON categories(poll_id);
            CREATE INDEX IF NOT EXISTS idx_candidates_poll ON candidates(poll_id);
            CREATE INDEX IF NOT EXISTS idx_vote_codes_poll ON vote_codes(poll_id);
            CREATE INDEX IF NOT EXISTS idx_ballots_poll ON ballots(poll_id);
            CREATE INDEX IF NOT EXISTS idx_selections_ballot ON selections(ballot_id);
            CREATE INDEX IF NOT EXISTS idx_selections_category_candidate ON selections(category_id, candidate_id);
            """
        )
    conn.commit()
    conn.close()


def esc(value):
    return html.escape(str(value), quote=True)


def make_public_id():
    return secrets.token_hex(6)


def make_vote_code():
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(7))


def generate_unique_codes(conn, poll_id, number):
    existing = {row["code"] for row in conn.execute("SELECT code FROM vote_codes").fetchall()}
    created = 0
    while created < number:
        code = make_vote_code()
        if code in existing:
            continue
        existing.add(code)
        conn.execute(
            "INSERT INTO vote_codes (poll_id, code, used) VALUES (?, ?, 0)",
            (poll_id, code),
        )
        created += 1


def get_category_results(conn, poll_id, category_id):
    return conn.execute(
        """
        SELECT c.id, c.name, COUNT(s.id) AS votes
        FROM candidates c
        LEFT JOIN selections s
          ON s.candidate_id = c.id
         AND s.category_id = ?
        WHERE c.poll_id = ?
        GROUP BY c.id, c.name
        ORDER BY votes DESC, c.name COLLATE NOCASE ASC
        """,
        (category_id, poll_id),
    ).fetchall()


def calculate_final_result(rows, places):
    positive = [row for row in rows if row["votes"] > 0]
    if not positive:
        return [], [], 0
    if len(positive) <= places:
        return positive, [], 0

    cutoff_votes = positive[places - 1]["votes"]
    selected = [row for row in positive if row["votes"] > cutoff_votes]
    tied = [row for row in positive if row["votes"] == cutoff_votes]
    remaining = places - len(selected)

    if len(tied) <= remaining:
        return selected + tied, [], 0
    return selected, tied, remaining


def page(title, content):
    return f"""<!DOCTYPE html>
<html lang="ro">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{esc(title)}</title>
<style>
* {{ box-sizing: border-box; }}
body {{ margin:0; font-family:Arial,Helvetica,sans-serif; background:#f3f5f7; color:#202124; }}
.container {{ width:min(1100px, calc(100% - 20px)); margin:24px auto; }}
.card {{ background:white; border:1px solid #d9dee5; border-radius:14px; padding:20px; margin-bottom:18px; }}
.narrow {{ max-width:580px; margin:50px auto; }}
.center {{ text-align:center; }}
.muted {{ color:#667085; }}
h1,h2,h3 {{ margin-top:0; }}
.topbar {{ display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:18px; }}
.button-group {{ display:flex; flex-wrap:wrap; gap:8px; }}
button,.button {{ border:0; border-radius:9px; padding:12px 16px; font-size:15px; font-weight:bold; cursor:pointer; text-decoration:none; display:inline-block; }}
.primary {{ background:#2457d6; color:white; }}
.secondary {{ background:#e9edf3; color:#202124; }}
.good {{ background:#067647; color:white; }}
.danger {{ background:#b42318; color:white; }}
.warning-button {{ background:#d97706; color:white; }}
.dark {{ background:#111827; color:white; }}
.big {{ width:100%; margin-top:20px; padding:15px; font-size:17px; }}
input,textarea {{ width:100%; padding:12px; margin-top:7px; border:1px solid #cdd3da; border-radius:9px; font-size:16px; }}
textarea {{ resize:vertical; }}
label {{ display:block; margin-top:15px; font-weight:bold; }}
.stats {{ display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:18px; }}
.stat {{ background:white; border:1px solid #d9dee5; border-radius:12px; padding:15px; text-align:center; }}
.stat strong {{ display:block; font-size:28px; }}
.category-row {{ display:grid; grid-template-columns:1fr 180px 55px; gap:10px; margin-bottom:10px; padding:12px; border:1px solid #dde2e8; border-radius:10px; align-items:end; }}
.remove {{ background:#fee4e2; color:#b42318; }}
.warning {{ background:#fff8e6; border:1px solid #f0c36d; border-radius:10px; padding:16px; margin-bottom:18px; }}
.danger-box {{ background:#fff1f0; border:2px solid #d92d20; border-radius:12px; padding:20px; margin-bottom:18px; }}
.final-box {{ background:#ecfdf3; border:2px solid #079455; }}
.tie-box {{ background:#fff8e6; border:2px solid #d97706; border-radius:12px; padding:15px; margin-top:15px; }}
.result,.elected-person,.tie-person {{ display:flex; justify-content:space-between; gap:12px; padding:10px 0; border-bottom:1px solid #e5e7eb; }}
.codes {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(130px,1fr)); gap:10px; }}
.code {{ border:1px solid #d9dee5; border-radius:9px; padding:12px; text-align:center; }}
.used {{ opacity:.45; background:#f5f5f5; }}
.inline-form {{ display:inline; }}
.copy-box {{ display:flex; gap:8px; }}
.copy-box input {{ margin:0; }}
.small-input {{ max-width:150px; }}
.management-box {{ border:1px solid #d7dce3; border-radius:12px; padding:18px; margin-top:18px; }}
.status-open {{ font-weight:bold; color:#067647; }}
.status-closed {{ font-weight:bold; color:#b42318; }}
.vote-header {{ border-bottom:1px solid #e5e7eb; padding-bottom:14px; margin-bottom:16px; }}
.counter {{ display:inline-block; background:#eef4ff; color:#173b85; padding:8px 13px; border-radius:30px; }}
.slots-title {{ font-size:14px; font-weight:bold; margin-bottom:8px; }}
.slots {{ display:flex; gap:8px; flex-wrap:wrap; margin-bottom:18px; }}
.slot {{ min-width:160px; min-height:48px; border:2px dashed #b8c0cc; border-radius:10px; padding:10px; display:flex; align-items:center; justify-content:center; color:#667085; background:#fafafa; }}
.slot.filled {{ border-style:solid; border-color:#2457d6; background:#eef4ff; color:#173b85; cursor:pointer; }}
.slot-number {{ margin-right:5px; }}
.search-info {{ margin-top:7px; color:#667085; font-size:13px; }}
.list-toggle {{ margin-top:10px; }}
.candidate-list {{ display:none; max-height:330px; overflow-y:auto; margin-top:10px; border:1px solid #d9dee5; border-radius:10px; }}
.candidate-list.visible {{ display:block; }}
.candidate {{ display:flex; align-items:center; gap:12px; padding:11px; margin:0; border-bottom:1px solid #e6e8ec; cursor:pointer; font-weight:normal; }}
.candidate.selected {{ background:#eef4ff; }}
.candidate input {{ width:20px; height:20px; margin:0; }}
.no-results {{ display:none; padding:15px; color:#667085; }}
@media(max-width:750px) {{ .category-row {{ grid-template-columns:1fr; }} .stats {{ grid-template-columns:repeat(2,1fr); }} .slot {{ min-width:calc(50% - 5px); flex:1; }} .copy-box {{ display:block; }} .copy-box button {{ width:100%; margin-top:8px; }} }}
@media print {{ body {{ background:white; }} .no-print {{ display:none!important; }} .container {{ width:100%; margin:0; }} .card {{ border:none; padding:0; margin-bottom:28px; }} }}
</style>
</head>
<body>
<div class="container">{content}</div>
</body>
</html>"""


class Handler(http.server.BaseHTTPRequestHandler):
    admin_sessions = set()
    active_codes = {}

    def log_message(self, format, *args):
        pass

    def send_html(self, content, status=200, headers=None):
        data = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)

    def redirect(self, location, headers=None):
        self.send_response(302)
        self.send_header("Location", location)
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()

    def read_form(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8")
        return urllib.parse.parse_qs(body, keep_blank_values=True)

    def get_cookie(self, name):
        raw = self.headers.get("Cookie", "")
        for item in raw.split(";"):
            item = item.strip()
            if "=" not in item:
                continue
            key, value = item.split("=", 1)
            if key == name:
                return value
        return None

    def is_admin(self):
        token = self.get_cookie("admin_session")
        return token in Handler.admin_sessions

    def require_admin(self):
        if not self.is_admin():
            self.redirect("/login")
            return False
        return True

    def do_GET(self):
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path

            if path == "/health":
                self.send_html("OK", 200)
                return
            if path == "/":
                self.redirect("/admin")
                return
            if path == "/login":
                self.login_page()
                return
            if path == "/logout":
                self.logout()
                return
            if path == "/admin":
                self.admin_page()
                return
            if path == "/create":
                self.create_page()
                return

            if path.startswith("/admin/poll/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[3] == "edit":
                    self.edit_page(int(parts[2]))
                    return
                if len(parts) == 4 and parts[3] == "codes":
                    self.codes_page(int(parts[2]))
                    return
                if len(parts) == 4 and parts[3] == "print":
                    self.print_results_page(int(parts[2]))
                    return
                if len(parts) == 4 and parts[3] == "reset":
                    self.reset_confirm_page(int(parts[2]))
                    return
                if len(parts) == 3:
                    self.admin_poll(int(parts[2]))
                    return

            if path.startswith("/vote/"):
                parts = path.strip("/").split("/")
                if len(parts) == 3 and parts[2] == "ballot":
                    self.ballot_page(parts[1])
                    return
                if len(parts) == 2:
                    self.vote_code_page(parts[1])
                    return

            self.send_html("Pagina nu există.", 404)
        except Exception as error:
            self.send_html(page("Eroare", f'<div class="card narrow"><h1>Eroare</h1><p>{esc(error)}</p></div>'), 500)

    def do_POST(self):
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path

            if path == "/login":
                self.login_post()
                return
            if path == "/create":
                self.create_post()
                return

            if path.startswith("/admin/poll/"):
                parts = path.strip("/").split("/")
                poll_id = int(parts[2])
                if len(parts) == 4 and parts[3] == "edit":
                    self.edit_post(poll_id)
                    return
                if len(parts) == 4 and parts[3] == "toggle":
                    self.toggle_poll(poll_id)
                    return
                if len(parts) == 4 and parts[3] == "add-codes":
                    self.add_codes(poll_id)
                    return
                if len(parts) == 4 and parts[3] == "reset":
                    self.reset_poll(poll_id)
                    return
                if len(parts) == 4 and parts[3] == "duplicate":
                    self.duplicate_poll(poll_id)
                    return

            if path.startswith("/vote/"):
                parts = path.strip("/").split("/")
                if len(parts) == 3 and parts[2] == "ballot":
                    self.ballot_post(parts[1])
                    return
                if len(parts) == 2:
                    self.vote_code_post(parts[1])
                    return

            self.send_html("Operație necunoscută.", 404)
        except Exception as error:
            self.send_html(page("Eroare", f'<div class="card narrow"><h1>Eroare</h1><p>{esc(error)}</p></div>'), 500)

    def login_page(self):
        self.send_html(page("Autentificare", """
        <div class="card narrow">
          <h1>Administrare vot</h1>
          <p class="muted">Introdu parola de administrator.</p>
          <form method="post">
            <label>Parolă</label>
            <input type="password" name="password" required autofocus>
            <button class="primary big">Intră în administrare</button>
          </form>
        </div>
        """))

    def login_post(self):
        form = self.read_form()
        password = form.get("password", [""])[0]
        if password != ADMIN_PASSWORD:
            self.send_html(page("Parolă incorectă", """
            <div class="card narrow center">
              <h1>Parolă incorectă</h1>
              <a class="button primary" href="/login">Încearcă din nou</a>
            </div>
            """))
            return

        token = secrets.token_hex(32)
        Handler.admin_sessions.add(token)
        self.redirect(
            "/admin",
            {"Set-Cookie": f"admin_session={token}; Path=/; HttpOnly; SameSite=Lax"},
        )

    def logout(self):
        token = self.get_cookie("admin_session")
        if token:
            Handler.admin_sessions.discard(token)
        self.redirect(
            "/login",
            {"Set-Cookie": "admin_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"},
        )

    def admin_page(self):
        if not self.require_admin():
            return
        conn = get_db()
        polls = conn.execute("SELECT * FROM polls ORDER BY id DESC").fetchall()
        cards = ""
        for poll in polls:
            ballots = conn.execute("SELECT COUNT(*) FROM ballots WHERE poll_id = ?", (poll["id"],)).fetchone()[0]
            status = "🟢 DESCHISĂ" if poll["is_open"] else "🔴 ÎNCHISĂ"
            cards += f"""
            <div class="card">
              <div class="topbar">
                <div>
                  <h2>{esc(poll['title'])}</h2>
                  <p class="muted">{status} · {ballots} voturi</p>
                </div>
                <div class="button-group">
                  <a class="button primary" href="/admin/poll/{poll['id']}">Deschide</a>
                  <a class="button secondary" href="/admin/poll/{poll['id']}/edit">Editează</a>
                </div>
              </div>
            </div>
            """
        conn.close()
        if not cards:
            cards = '<div class="card">Nu există încă nicio votare.</div>'

        self.send_html(page("Votări", f"""
        <div class="topbar">
          <div><h1>Votări</h1><p class="muted">Panoul de administrare.</p></div>
          <div class="button-group">
            <a class="button primary" href="/create">+ Votare nouă</a>
            <a class="button secondary" href="/logout">Ieșire</a>
          </div>
        </div>
        {cards}
        """))

    def create_page(self):
        if not self.require_admin():
            return
        self.send_html(page("Creează votarea", """
        <div class="topbar">
          <div><h1>Creează votarea</h1><p class="muted">Poți adăuga oricâte domenii.</p></div>
          <a class="button secondary" href="/admin">Înapoi</a>
        </div>
        <form method="post" class="card">
          <label>Titlul votării</label>
          <input name="title" required placeholder="Ex: Alegerea reprezentanților">

          <div class="topbar" style="margin-top:25px">
            <h2>Domenii de vot</h2>
            <button type="button" class="secondary" id="addCategoryBtn">+ Adaugă domeniu</button>
          </div>

          <div id="categories">
            <div class="category-row">
              <div><label>Domeniul</label><input name="category_name" required placeholder="Ex: Consiliul de administrație"></div>
              <div><label>Nr. persoane de ales</label><input name="max_choices" type="number" value="1" min="1" required></div>
              <button type="button" class="remove remove-category">×</button>
            </div>
          </div>

          <label>Lista persoanelor</label>
          <textarea name="candidates" rows="14" required placeholder="Lipește lista din Excel. Un nume pe fiecare rând."></textarea>
          <p class="muted">Poți copia direct o coloană din Excel.</p>

          <label>Număr coduri unice</label>
          <input type="number" name="code_count" value="100" min="1" max="5000" required>
          <button class="primary big">GENEREAZĂ VOTAREA</button>
        </form>

        <script>
        const categories = document.getElementById('categories');
        document.getElementById('addCategoryBtn').addEventListener('click', function () {
            const row = document.createElement('div');
            row.className = 'category-row';
            row.innerHTML = `
              <div><label>Domeniul</label><input name="category_name" required placeholder="Denumirea domeniului"></div>
              <div><label>Nr. persoane de ales</label><input name="max_choices" type="number" value="1" min="1" required></div>
              <button type="button" class="remove remove-category">×</button>
            `;
            categories.appendChild(row);
        });
        categories.addEventListener('click', function (event) {
            if (!event.target.classList.contains('remove-category')) return;
            if (categories.children.length > 1) event.target.closest('.category-row').remove();
        });
        </script>
        """))

    def create_post(self):
        if not self.require_admin():
            return
        form = self.read_form()
        title = form.get("title", [""])[0].strip() or "Votare"
        category_names = form.get("category_name", [])
        maximums = form.get("max_choices", [])
        candidates_text = form.get("candidates", [""])[0]

        try:
            code_count = int(form.get("code_count", ["100"])[0])
        except ValueError:
            code_count = 100
        code_count = max(1, min(code_count, 5000))

        candidates = []
        seen = set()
        for line in candidates_text.splitlines():
            name = line.strip()
            if not name:
                continue
            key = name.casefold()
            if key in seen:
                continue
            seen.add(key)
            candidates.append(name)

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO polls (public_id, title, is_open, created_at) VALUES (?, ?, 1, ?)",
            (make_public_id(), title, datetime.now().isoformat()),
        )
        poll_id = cur.lastrowid

        order = 0
        for index, name in enumerate(category_names):
            name = name.strip()
            if not name:
                continue
            try:
                maximum = int(maximums[index])
            except (ValueError, IndexError):
                maximum = 1
            maximum = max(1, maximum)
            cur.execute(
                "INSERT INTO categories (poll_id, name, max_choices, sort_order) VALUES (?, ?, ?, ?)",
                (poll_id, name, maximum, order),
            )
            order += 1

        for index, name in enumerate(candidates):
            cur.execute(
                "INSERT INTO candidates (poll_id, name, sort_order) VALUES (?, ?, ?)",
                (poll_id, name, index),
            )

        generate_unique_codes(conn, poll_id, code_count)
        conn.commit()
        conn.close()
        self.redirect(f"/admin/poll/{poll_id}")

    def edit_page(self, poll_id):
        if not self.require_admin():
            return
        conn = get_db()
        poll = conn.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()
        if not poll:
            conn.close()
            self.send_html("Votarea nu există.", 404)
            return
        categories = conn.execute("SELECT * FROM categories WHERE poll_id = ? ORDER BY sort_order", (poll_id,)).fetchall()
        candidates = conn.execute("SELECT * FROM candidates WHERE poll_id = ? ORDER BY sort_order", (poll_id,)).fetchall()
        ballot_count = conn.execute("SELECT COUNT(*) FROM ballots WHERE poll_id = ?", (poll_id,)).fetchone()[0]
        conn.close()

        locked = ballot_count > 0
        disabled = "disabled" if locked else ""
        warning = ""
        if locked:
            warning = f'<div class="warning"><strong>Atenție:</strong> există deja {ballot_count} voturi. Titlul poate fi modificat, dar domeniile și candidații sunt blocați. Pentru reluare, folosește Resetare votare.</div>'

        category_html = ""
        for category in categories:
            category_html += f"""
            <div class="category-row">
              <div><label>Domeniul</label><input name="category_name" value="{esc(category['name'])}" required {disabled}></div>
              <div><label>Nr. persoane de ales</label><input name="max_choices" type="number" value="{category['max_choices']}" min="1" required {disabled}></div>
              <button type="button" class="remove remove-category" {disabled}>×</button>
            </div>
            """

        candidates_text = "\n".join(row["name"] for row in candidates)
        locked_js = "true" if locked else "false"

        self.send_html(page("Editează votarea", f"""
        <div class="topbar">
          <div><h1>Editează votarea</h1><p class="muted">{esc(poll['title'])}</p></div>
          <a class="button secondary" href="/admin/poll/{poll_id}">Înapoi</a>
        </div>
        {warning}
        <form method="post" class="card">
          <label>Titlul votării</label>
          <input name="title" value="{esc(poll['title'])}" required>

          <div class="topbar" style="margin-top:25px">
            <h2>Domenii</h2>
            <button type="button" class="secondary" id="addCategoryBtn" {disabled}>+ Adaugă domeniu</button>
          </div>
          <div id="categories">{category_html}</div>

          <label>Lista persoanelor</label>
          <textarea name="candidates" rows="15" {disabled}>{esc(candidates_text)}</textarea>
          <button class="primary big">SALVEAZĂ MODIFICĂRILE</button>
        </form>

        <script>
        const locked = {locked_js};
        const categories = document.getElementById('categories');
        const addBtn = document.getElementById('addCategoryBtn');
        if (addBtn) {{
            addBtn.addEventListener('click', function () {{
                if (locked) return;
                const row = document.createElement('div');
                row.className = 'category-row';
                row.innerHTML = `
                  <div><label>Domeniul</label><input name="category_name" required></div>
                  <div><label>Nr. persoane de ales</label><input name="max_choices" type="number" value="1" min="1" required></div>
                  <button type="button" class="remove remove-category">×</button>
                `;
                categories.appendChild(row);
            }});
        }}
        categories.addEventListener('click', function (event) {{
            if (locked) return;
            if (!event.target.classList.contains('remove-category')) return;
            if (categories.children.length > 1) event.target.closest('.category-row').remove();
        }});
        </script>
        """))

    def edit_post(self, poll_id):
        if not self.require_admin():
            return
        form = self.read_form()
        conn = get_db()
        cur = conn.cursor()
        poll = cur.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()
        if not poll:
            conn.close()
            self.send_html("Votarea nu există.", 404)
            return

        title = form.get("title", [poll["title"]])[0].strip() or poll["title"]
        cur.execute("UPDATE polls SET title = ? WHERE id = ?", (title, poll_id))
        ballot_count = cur.execute("SELECT COUNT(*) FROM ballots WHERE poll_id = ?", (poll_id,)).fetchone()[0]

        if ballot_count == 0:
            category_names = form.get("category_name", [])
            maximums = form.get("max_choices", [])
            candidates_text = form.get("candidates", [""])[0]

            cur.execute("DELETE FROM categories WHERE poll_id = ?", (poll_id,))
            cur.execute("DELETE FROM candidates WHERE poll_id = ?", (poll_id,))

            order = 0
            for index, name in enumerate(category_names):
                name = name.strip()
                if not name:
                    continue
                try:
                    maximum = int(maximums[index])
                except (ValueError, IndexError):
                    maximum = 1
                cur.execute(
                    "INSERT INTO categories (poll_id, name, max_choices, sort_order) VALUES (?, ?, ?, ?)",
                    (poll_id, name, max(1, maximum), order),
                )
                order += 1

            seen = set()
            candidates = []
            for line in candidates_text.splitlines():
                name = line.strip()
                if not name:
                    continue
                key = name.casefold()
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(name)

            for index, name in enumerate(candidates):
                cur.execute(
                    "INSERT INTO candidates (poll_id, name, sort_order) VALUES (?, ?, ?)",
                    (poll_id, name, index),
                )

        conn.commit()
        conn.close()
        self.redirect(f"/admin/poll/{poll_id}")

    def admin_poll(self, poll_id):
        if not self.require_admin():
            return
        conn = get_db()
        poll = conn.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()
        if not poll:
            conn.close()
            self.send_html("Votarea nu există.", 404)
            return

        total_codes = conn.execute("SELECT COUNT(*) FROM vote_codes WHERE poll_id = ?", (poll_id,)).fetchone()[0]
        used_codes = conn.execute("SELECT COUNT(*) FROM vote_codes WHERE poll_id = ? AND used = 1", (poll_id,)).fetchone()[0]
        ballots = conn.execute("SELECT COUNT(*) FROM ballots WHERE poll_id = ?", (poll_id,)).fetchone()[0]
        candidate_count = conn.execute("SELECT COUNT(*) FROM candidates WHERE poll_id = ?", (poll_id,)).fetchone()[0]
        categories = conn.execute("SELECT * FROM categories WHERE poll_id = ? ORDER BY sort_order", (poll_id,)).fetchall()

        host = self.headers.get("Host", f"127.0.0.1:{PORT}")
        scheme = self.headers.get("X-Forwarded-Proto", "http")
        vote_link = f"{scheme}://{host}/vote/{poll['public_id']}"

        if poll["is_open"]:
            status_html = '<span class="status-open">🟢 VOTAREA ESTE DESCHISĂ</span>'
            toggle_text = "Închide votarea"
            toggle_class = "danger"
            result_html = '<div class="card"><h2>Rezultatele finale</h2><p class="muted">Persoanele declarate alese vor fi afișate după închiderea votării.</p></div>'
        else:
            status_html = '<span class="status-closed">🔴 VOTAREA ESTE ÎNCHISĂ</span>'
            toggle_text = "Redeschide votarea"
            toggle_class = "good"
            result_html = '<div class="card final-box"><h1>🏆 REZULTAT FINAL</h1><p>Persoanele alese sunt afișate mai jos.</p></div>'

            for category in categories:
                rows = get_category_results(conn, poll_id, category["id"])
                selected, tied, remaining = calculate_final_result(rows, category["max_choices"])

                selected_html = "".join(
                    f'<div class="elected-person"><strong>✅ {esc(row["name"])}</strong><strong>{row["votes"]} voturi</strong></div>'
                    for row in selected
                ) or '<p class="muted">Nu există persoane alese sigur.</p>'

                tie_html = ""
                if tied:
                    people = "".join(
                        f'<div class="tie-person"><strong>⚠️ {esc(row["name"])}</strong><strong>{row["votes"]} voturi</strong></div>'
                        for row in tied
                    )
                    place_text = "1 loc" if remaining == 1 else f"{remaining} locuri"
                    tie_html = f'<div class="tie-box"><h3>⚠️ EGALITATE</h3><p>Egalitate pentru <strong>{place_text}</strong>.</p>{people}</div>'

                positive = [row for row in rows if row["votes"] > 0]
                ranking = ""
                last_votes = None
                rank = 0
                for index, row in enumerate(positive, start=1):
                    if last_votes != row["votes"]:
                        rank = index
                    ranking += f'<div class="result"><span>{rank}. {esc(row["name"])}</span><strong>{row["votes"]}</strong></div>'
                    last_votes = row["votes"]
                if not ranking:
                    ranking = '<p class="muted">Nu există voturi.</p>'

                result_html += f"""
                <div class="card">
                  <h2>{esc(category['name'])}</h2>
                  <p class="muted">Locuri: <strong>{category['max_choices']}</strong></p>
                  <h3>Persoane alese</h3>
                  {selected_html}
                  {tie_html}
                  <h3 style="margin-top:25px">Clasament complet</h3>
                  {ranking}
                </div>
                """

        conn.close()

        print_button = ""
        if not poll["is_open"]:
            print_button = f'<a class="button dark" target="_blank" href="/admin/poll/{poll_id}/print">🖨 PRINTEAZĂ REZULTATELE</a>'

        self.send_html(page(poll["title"], f"""
        <div class="topbar">
          <div><h1>{esc(poll['title'])}</h1><p>{status_html}</p></div>
          <div class="button-group">
            <a class="button secondary" href="/admin">Înapoi</a>
            <a class="button primary" href="/admin/poll/{poll_id}/edit">Editează</a>
            {print_button}
          </div>
        </div>

        <div class="stats">
          <div class="stat"><strong>{ballots}</strong>voturi</div>
          <div class="stat"><strong>{used_codes}</strong>coduri folosite</div>
          <div class="stat"><strong>{total_codes - used_codes}</strong>coduri libere</div>
          <div class="stat"><strong>{candidate_count}</strong>persoane</div>
        </div>

        <div class="card">
          <h2>Link pentru vot</h2>
          <div class="copy-box">
            <input id="voteLink" value="{esc(vote_link)}" readonly onclick="this.select()">
            <button type="button" class="secondary" id="copyLinkBtn">Copiază</button>
          </div>
          <br>
          <div class="button-group">
            <a class="button secondary" href="/admin/poll/{poll_id}/codes">Vezi codurile</a>
            <form method="post" action="/admin/poll/{poll_id}/toggle" class="inline-form"><button class="{toggle_class}">{toggle_text}</button></form>
          </div>
        </div>

        <div class="card">
          <h2>Adaugă coduri suplimentare</h2>
          <form method="post" action="/admin/poll/{poll_id}/add-codes">
            <label>Câte coduri noi?</label>
            <input class="small-input" type="number" name="amount" value="10" min="1" max="5000" required>
            <br><button class="primary">Generează coduri</button>
          </form>
        </div>

        <div class="card">
          <h2>Instrumente votare</h2>
          <div class="management-box">
            <h3>🔄 Resetare votare</h3>
            <p>Șterge voturile existente, păstrează domeniile și candidații și generează coduri complet noi.</p>
            <a class="button danger" href="/admin/poll/{poll_id}/reset">RESETARE VOTARE</a>
          </div>
          <div class="management-box">
            <h3>📄 Duplică votarea</h3>
            <p>Creează o copie nouă fără voturi. Votarea actuală și rezultatele ei rămân intacte.</p>
            <form method="post" action="/admin/poll/{poll_id}/duplicate"><button class="warning-button">DUPLICĂ VOTAREA</button></form>
          </div>
        </div>

        {result_html}

        <script>
        document.getElementById('copyLinkBtn').addEventListener('click', function () {{
            const input = document.getElementById('voteLink');
            input.select();
            if (navigator.clipboard) {{
                navigator.clipboard.writeText(input.value).then(function () {{ alert('Linkul a fost copiat.'); }});
            }} else {{
                document.execCommand('copy');
                alert('Linkul a fost copiat.');
            }}
        }});
        </script>
        """))

    def reset_confirm_page(self, poll_id):
        if not self.require_admin():
            return
        conn = get_db()
        poll = conn.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()
        if not poll:
            conn.close()
            self.send_html("Votarea nu există.", 404)
            return
        ballots = conn.execute("SELECT COUNT(*) FROM ballots WHERE poll_id = ?", (poll_id,)).fetchone()[0]
        codes = conn.execute("SELECT COUNT(*) FROM vote_codes WHERE poll_id = ?", (poll_id,)).fetchone()[0]
        conn.close()

        self.send_html(page("Confirmare resetare", f"""
        <div class="card narrow">
          <div class="danger-box">
            <h1>⚠️ RESETARE VOTARE</h1>
            <h2>{esc(poll['title'])}</h2>
            <p>Se vor șterge definitiv <strong>{ballots}</strong> voturi.</p>
            <p>Cele <strong>{codes}</strong> coduri actuale vor fi eliminate și vor fi generate coduri noi.</p>
            <p>Vor rămâne titlul, domeniile, numărul de locuri și lista persoanelor.</p>
          </div>
          <form method="post" action="/admin/poll/{poll_id}/reset" id="resetForm">
            <button class="danger big">DA, RESETEAZĂ VOTAREA</button>
          </form>
          <a class="button secondary big center" href="/admin/poll/{poll_id}">NU, ÎNAPOI</a>
        </div>
        <script>
        document.getElementById('resetForm').addEventListener('submit', function (event) {{
            if (!confirm('Ești sigur că vrei să ștergi definitiv toate voturile?')) event.preventDefault();
        }});
        </script>
        """))

    def reset_poll(self, poll_id):
        if not self.require_admin():
            return
        conn = get_db()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")
            poll = cur.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()
            if not poll:
                conn.rollback()
                conn.close()
                self.send_html("Votarea nu există.", 404)
                return

            code_count = cur.execute("SELECT COUNT(*) FROM vote_codes WHERE poll_id = ?", (poll_id,)).fetchone()[0]
            if code_count < 1:
                code_count = 100

            ballot_ids = [row["id"] for row in cur.execute("SELECT id FROM ballots WHERE poll_id = ?", (poll_id,)).fetchall()]
            if ballot_ids:
                placeholders = ",".join("?" for _ in ballot_ids)
                cur.execute(f"DELETE FROM selections WHERE ballot_id IN ({placeholders})", ballot_ids)

            cur.execute("DELETE FROM ballots WHERE poll_id = ?", (poll_id,))
            cur.execute("DELETE FROM vote_codes WHERE poll_id = ?", (poll_id,))
            generate_unique_codes(conn, poll_id, code_count)
            cur.execute("UPDATE polls SET is_open = 1 WHERE id = ?", (poll_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise
        conn.close()

        for token in [t for t, data in Handler.active_codes.items() if data.get("poll_id") == poll_id]:
            Handler.active_codes.pop(token, None)

        self.redirect(f"/admin/poll/{poll_id}/codes")

    def duplicate_poll(self, poll_id):
        if not self.require_admin():
            return
        conn = get_db()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")
            original = cur.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()
            if not original:
                conn.rollback()
                conn.close()
                self.send_html("Votarea nu există.", 404)
                return

            categories = cur.execute("SELECT * FROM categories WHERE poll_id = ? ORDER BY sort_order", (poll_id,)).fetchall()
            candidates = cur.execute("SELECT * FROM candidates WHERE poll_id = ? ORDER BY sort_order", (poll_id,)).fetchall()
            code_count = cur.execute("SELECT COUNT(*) FROM vote_codes WHERE poll_id = ?", (poll_id,)).fetchone()[0]
            if code_count < 1:
                code_count = 100

            cur.execute(
                "INSERT INTO polls (public_id, title, is_open, created_at) VALUES (?, ?, 1, ?)",
                (make_public_id(), original["title"] + " - copie", datetime.now().isoformat()),
            )
            new_poll_id = cur.lastrowid

            for category in categories:
                cur.execute(
                    "INSERT INTO categories (poll_id, name, max_choices, sort_order) VALUES (?, ?, ?, ?)",
                    (new_poll_id, category["name"], category["max_choices"], category["sort_order"]),
                )
            for candidate in candidates:
                cur.execute(
                    "INSERT INTO candidates (poll_id, name, sort_order) VALUES (?, ?, ?)",
                    (new_poll_id, candidate["name"], candidate["sort_order"]),
                )
            generate_unique_codes(conn, new_poll_id, code_count)
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise
        conn.close()
        self.redirect(f"/admin/poll/{new_poll_id}")

    def add_codes(self, poll_id):
        if not self.require_admin():
            return
        form = self.read_form()
        try:
            amount = int(form.get("amount", ["10"])[0])
        except ValueError:
            amount = 10
        amount = max(1, min(amount, 5000))
        conn = get_db()
        generate_unique_codes(conn, poll_id, amount)
        conn.commit()
        conn.close()
        self.redirect(f"/admin/poll/{poll_id}/codes")

    def codes_page(self, poll_id):
        if not self.require_admin():
            return
        conn = get_db()
        poll = conn.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()
        if not poll:
            conn.close()
            self.send_html("Votarea nu există.", 404)
            return
        codes = conn.execute("SELECT * FROM vote_codes WHERE poll_id = ? ORDER BY id", (poll_id,)).fetchall()
        conn.close()

        code_html = ""
        for row in codes:
            css = "code used" if row["used"] else "code"
            status = "FOLOSIT" if row["used"] else "DISPONIBIL"
            code_html += f'<div class="{css}"><strong>{esc(row["code"])}</strong><br><small>{status}</small></div>'

        self.send_html(page("Coduri", f"""
        <div class="topbar">
          <div><h1>Coduri de vot</h1><p class="muted">{esc(poll['title'])}</p></div>
          <a class="button secondary" href="/admin/poll/{poll_id}">Înapoi</a>
        </div>
        <div class="card">
          <p>Fiecare cod poate fi folosit o singură dată.</p>
          <div class="codes">{code_html}</div>
        </div>
        """))

    def toggle_poll(self, poll_id):
        if not self.require_admin():
            return
        conn = get_db()
        row = conn.execute("SELECT is_open FROM polls WHERE id = ?", (poll_id,)).fetchone()
        if row:
            conn.execute("UPDATE polls SET is_open = ? WHERE id = ?", (0 if row["is_open"] else 1, poll_id))
            conn.commit()
        conn.close()
        self.redirect(f"/admin/poll/{poll_id}")

    def print_results_page(self, poll_id):
        if not self.require_admin():
            return
        conn = get_db()
        poll = conn.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()
        if not poll:
            conn.close()
            self.send_html("Votarea nu există.", 404)
            return
        if poll["is_open"]:
            conn.close()
            self.send_html(page("Votare deschisă", f'<div class="card narrow center"><h1>Votarea este încă deschisă</h1><p>Închide votarea înainte de tipărirea rezultatelor.</p><a class="button primary" href="/admin/poll/{poll_id}">Înapoi</a></div>'))
            return

        ballots = conn.execute("SELECT COUNT(*) FROM ballots WHERE poll_id = ?", (poll_id,)).fetchone()[0]
        categories = conn.execute("SELECT * FROM categories WHERE poll_id = ? ORDER BY sort_order", (poll_id,)).fetchall()
        result_html = ""

        for category in categories:
            rows = get_category_results(conn, poll_id, category["id"])
            selected, tied, remaining = calculate_final_result(rows, category["max_choices"])

            selected_html = "".join(
                f'<div class="elected-person"><strong>{esc(row["name"])}</strong><span>{row["votes"]} voturi</span></div>'
                for row in selected
            ) or '<p>Nu există persoane alese sigur.</p>'

            tie_html = ""
            if tied:
                people = "".join(
                    f'<div class="tie-person"><strong>{esc(row["name"])}</strong><span>{row["votes"]} voturi</span></div>'
                    for row in tied
                )
                places = "1 LOC" if remaining == 1 else f"{remaining} LOCURI"
                tie_html = f'<div class="tie-box"><h3>EGALITATE PENTRU {places}</h3>{people}</div>'

            positive = [row for row in rows if row["votes"] > 0]
            ranking = ""
            last_votes = None
            rank = 0
            for index, row in enumerate(positive, start=1):
                if last_votes != row["votes"]:
                    rank = index
                ranking += f'<div class="result"><span>{rank}. {esc(row["name"])}</span><strong>{row["votes"]} voturi</strong></div>'
                last_votes = row["votes"]
            if not ranking:
                ranking = '<p>Nu există voturi.</p>'

            result_html += f"""
            <div class="card">
              <h2>{esc(category['name'])}</h2>
              <p>Număr de persoane de ales: <strong>{category['max_choices']}</strong></p>
              <h3>Persoane alese</h3>
              {selected_html}
              {tie_html}
              <h3 style="margin-top:25px">Clasament complet</h3>
              {ranking}
            </div>
            """

        conn.close()
        date_text = datetime.now().strftime("%d.%m.%Y, %H:%M")
        self.send_html(page("Rezultate finale", f"""
        <div class="no-print topbar">
          <a class="button secondary" href="/admin/poll/{poll_id}">← Înapoi</a>
          <button class="dark" id="printBtn">🖨 PRINTEAZĂ</button>
        </div>
        <div class="card center">
          <h1>REZULTATE FINALE</h1>
          <h2>{esc(poll['title'])}</h2>
          <p>Data: <strong>{date_text}</strong></p>
          <p>Număr total de voturi: <strong>{ballots}</strong></p>
        </div>
        {result_html}
        <div style="margin-top:60px;display:flex;justify-content:space-between;gap:60px">
          <div style="width:45%;text-align:center">____________________________<br><br>Secretariat</div>
          <div style="width:45%;text-align:center">____________________________<br><br>Semnătură</div>
        </div>
        <script>document.getElementById('printBtn').addEventListener('click', function () {{ window.print(); }});</script>
        """))

    def vote_code_page(self, public_id):
        conn = get_db()
        poll = conn.execute("SELECT * FROM polls WHERE public_id = ?", (public_id,)).fetchone()
        conn.close()
        if not poll:
            self.send_html("Votarea nu există.", 404)
            return
        if not poll["is_open"]:
            self.send_html(page("Votare închisă", '<div class="card narrow center"><h1>Votarea este închisă</h1><p class="muted">Nu se mai pot înregistra voturi.</p></div>'))
            return

        self.send_html(page(poll["title"], f"""
        <div class="card narrow">
          <h1>{esc(poll['title'])}</h1>
          <p class="muted">Introdu codul unic primit.</p>
          <form method="post">
            <label>Cod unic</label>
            <input name="code" required autofocus autocomplete="off" style="text-align:center;font-size:22px;letter-spacing:4px;text-transform:uppercase">
            <button class="primary big">Continuă</button>
          </form>
        </div>
        """))

    def vote_code_post(self, public_id):
        form = self.read_form()
        code = form.get("code", [""])[0].strip().upper().replace(" ", "")
        conn = get_db()
        poll = conn.execute("SELECT * FROM polls WHERE public_id = ?", (public_id,)).fetchone()
        if not poll:
            conn.close()
            self.send_html("Votarea nu există.", 404)
            return
        if not poll["is_open"]:
            conn.close()
            self.send_html(page("Votare închisă", '<div class="card narrow center"><h1>Votarea este închisă.</h1></div>'))
            return

        vote_code = conn.execute("SELECT * FROM vote_codes WHERE poll_id = ? AND code = ?", (poll["id"], code)).fetchone()
        conn.close()
        if not vote_code:
            self.send_html(page("Cod invalid", f'<div class="card narrow center"><h1>Cod invalid</h1><p>Codul introdus nu există.</p><a class="button primary" href="/vote/{public_id}">Încearcă din nou</a></div>'))
            return
        if vote_code["used"]:
            self.send_html(page("Cod folosit", f'<div class="card narrow center"><h1>Cod deja folosit</h1><p>Acest cod nu mai poate fi utilizat.</p><a class="button primary" href="/vote/{public_id}">Înapoi</a></div>'))
            return

        token = secrets.token_hex(24)
        Handler.active_codes[token] = {"code_id": vote_code["id"], "poll_id": poll["id"]}
        self.redirect(f"/vote/{public_id}/ballot?token={token}")

    def ballot_page(self, public_id):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        token = params.get("token", [""])[0]
        session = Handler.active_codes.get(token)
        if not session:
            self.redirect(f"/vote/{public_id}")
            return

        conn = get_db()
        poll = conn.execute("SELECT * FROM polls WHERE public_id = ?", (public_id,)).fetchone()
        if not poll:
            conn.close()
            self.send_html("Votarea nu există.", 404)
            return
        if not poll["is_open"]:
            conn.close()
            self.send_html(page("Închis", '<div class="card narrow center"><h1>Votarea este închisă.</h1></div>'))
            return

        vote_code = conn.execute("SELECT * FROM vote_codes WHERE id = ?", (session["code_id"],)).fetchone()
        if not vote_code or vote_code["used"] or vote_code["poll_id"] != poll["id"]:
            conn.close()
            Handler.active_codes.pop(token, None)
            self.redirect(f"/vote/{public_id}")
            return

        categories = conn.execute("SELECT * FROM categories WHERE poll_id = ? ORDER BY sort_order", (poll["id"],)).fetchall()
        candidates = conn.execute("SELECT * FROM candidates WHERE poll_id = ? ORDER BY name COLLATE NOCASE", (poll["id"],)).fetchall()
        conn.close()

        sections = ""
        for category in categories:
            people_html = ""
            for candidate in candidates:
                safe_name = esc(candidate["name"])
                search_name = esc(candidate["name"].casefold())
                people_html += f"""
                <label class="candidate" data-name="{search_name}">
                  <input type="checkbox" name="category_{category['id']}" value="{candidate['id']}" data-person-name="{safe_name}">
                  <span>{safe_name}</span>
                </label>
                """

            slots = "".join(
                f'<div class="slot" data-candidate-id=""><span class="slot-number">{number}.</span><span class="slot-text">Alege persoana</span></div>'
                for number in range(1, category["max_choices"] + 1)
            )

            sections += f"""
            <div class="card vote-section" data-max="{category['max_choices']}">
              <div class="vote-header">
                <div class="topbar">
                  <div><h2>{esc(category['name'])}</h2><p class="muted">Număr de persoane de ales: <strong>{category['max_choices']}</strong></p></div>
                  <strong class="counter">0 / {category['max_choices']}</strong>
                </div>
                <div class="slots-title">Persoane selectate</div>
                <div class="slots">{slots}</div>
              </div>
              <input type="search" class="candidate-search" placeholder="🔎 Caută persoana..." autocomplete="off">
              <div class="search-info">Scrie numele pentru a afișa rezultatele.</div>
              <button type="button" class="secondary list-toggle">Afișează lista completă</button>
              <div class="candidate-list">{people_html}<div class="no-results">Nu am găsit nicio persoană.</div></div>
            </div>
            """

        self.send_html(page(poll["title"], f"""
        <h1>{esc(poll['title'])}</h1>
        <p class="muted">Selectează persoanele dorite.</p>
        <form method="post" action="/vote/{public_id}/ballot?token={token}">
          {sections}
          <div class="card">
            <button class="primary big">TRIMITE VOTUL</button>
            <p class="muted center">După trimitere, codul nu mai poate fi utilizat.</p>
          </div>
        </form>

        <script>
        document.querySelectorAll('.vote-section').forEach(function (section) {{
            const max = Number(section.dataset.max);
            const boxes = Array.from(section.querySelectorAll('.candidate input[type="checkbox"]'));
            const rows = Array.from(section.querySelectorAll('.candidate'));
            const slots = Array.from(section.querySelectorAll('.slot'));
            const counter = section.querySelector('.counter');
            const search = section.querySelector('.candidate-search');
            const list = section.querySelector('.candidate-list');
            const toggle = section.querySelector('.list-toggle');
            const noResults = section.querySelector('.no-results');
            let fullList = false;

            function selectedBoxes() {{
                return boxes.filter(function (box) {{ return box.checked; }});
            }}

            function refresh() {{
                const selected = selectedBoxes();
                counter.textContent = selected.length + ' / ' + max;

                slots.forEach(function (slot, index) {{
                    const text = slot.querySelector('.slot-text');
                    if (selected[index]) {{
                        text.textContent = selected[index].dataset.personName;
                        slot.classList.add('filled');
                        slot.dataset.candidateId = selected[index].value;
                    }} else {{
                        text.textContent = 'Alege persoana';
                        slot.classList.remove('filled');
                        slot.dataset.candidateId = '';
                    }}
                }});

                rows.forEach(function (row) {{
                    const box = row.querySelector('input[type="checkbox"]');
                    row.classList.toggle('selected', box.checked);
                }});
            }}

            function filterPeople() {{
                const value = search.value.toLocaleLowerCase('ro').trim();
                if (!value && !fullList) {{
                    list.classList.remove('visible');
                    noResults.style.display = 'none';
                    return;
                }}

                list.classList.add('visible');
                let count = 0;
                rows.forEach(function (row) {{
                    const visible = !value || row.dataset.name.includes(value);
                    row.style.display = visible ? 'flex' : 'none';
                    if (visible) count += 1;
                }});
                noResults.style.display = count === 0 ? 'block' : 'none';
            }}

            boxes.forEach(function (box) {{
                box.addEventListener('change', function () {{
                    if (selectedBoxes().length > max) {{
                        box.checked = false;
                        alert('Poți selecta maximum ' + max + ' persoane.');
                    }}
                    refresh();
                }});
            }});

            search.addEventListener('input', function () {{
                fullList = false;
                toggle.textContent = 'Afișează lista completă';
                filterPeople();
            }});

            toggle.addEventListener('click', function () {{
                fullList = !fullList;
                if (fullList) {{
                    search.value = '';
                    toggle.textContent = 'Ascunde lista';
                }} else {{
                    toggle.textContent = 'Afișează lista completă';
                }}
                filterPeople();
            }});

            slots.forEach(function (slot) {{
                slot.addEventListener('click', function () {{
                    const id = slot.dataset.candidateId;
                    if (!id) {{
                        search.focus();
                        return;
                    }}
                    const box = boxes.find(function (item) {{ return item.value === id; }});
                    if (box) box.checked = false;
                    refresh();
                }});
            }});

            refresh();
        }});
        </script>
        """))

    def ballot_post(self, public_id):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        token = params.get("token", [""])[0]
        session = Handler.active_codes.get(token)
        if not session:
            self.redirect(f"/vote/{public_id}")
            return

        form = self.read_form()
        conn = get_db()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")
            poll = cur.execute("SELECT * FROM polls WHERE public_id = ?", (public_id,)).fetchone()
            if not poll or not poll["is_open"]:
                conn.rollback()
                conn.close()
                self.send_html(page("Votare închisă", '<div class="card narrow center"><h1>Votarea nu este disponibilă.</h1></div>'))
                return

            vote_code = cur.execute("SELECT * FROM vote_codes WHERE id = ? AND poll_id = ?", (session["code_id"], poll["id"])).fetchone()
            if not vote_code or vote_code["used"]:
                conn.rollback()
                conn.close()
                Handler.active_codes.pop(token, None)
                self.send_html(page("Cod folosit", '<div class="card narrow center"><h1>Codul nu mai este disponibil.</h1></div>'))
                return

            categories = cur.execute("SELECT * FROM categories WHERE poll_id = ?", (poll["id"],)).fetchall()
            selections = []
            for category in categories:
                selected_raw = form.get(f"category_{category['id']}", [])
                unique = []
                for value in selected_raw:
                    try:
                        candidate_id = int(value)
                    except ValueError:
                        continue
                    if candidate_id not in unique:
                        unique.append(candidate_id)

                if len(unique) > category["max_choices"]:
                    conn.rollback()
                    conn.close()
                    self.send_html(page("Selecție invalidă", '<div class="card narrow center"><h1>Ai selectat prea multe persoane.</h1></div>'))
                    return

                for candidate_id in unique:
                    valid = cur.execute("SELECT id FROM candidates WHERE id = ? AND poll_id = ?", (candidate_id, poll["id"])).fetchone()
                    if valid:
                        selections.append((category["id"], candidate_id))

            cur.execute(
                "UPDATE vote_codes SET used = 1, used_at = ? WHERE id = ? AND poll_id = ? AND used = 0",
                (datetime.now().isoformat(), vote_code["id"], poll["id"]),
            )
            if cur.rowcount != 1:
                conn.rollback()
                conn.close()
                self.send_html(page("Cod utilizat", '<div class="card narrow center"><h1>Codul a fost deja utilizat.</h1></div>'))
                return

            cur.execute("INSERT INTO ballots (poll_id, submitted_at) VALUES (?, ?)", (poll["id"], datetime.now().isoformat()))
            ballot_id = cur.lastrowid
            for category_id, candidate_id in selections:
                cur.execute(
                    "INSERT INTO selections (ballot_id, category_id, candidate_id) VALUES (?, ?, ?)",
                    (ballot_id, category_id, candidate_id),
                )

            conn.commit()
            conn.close()
        except Exception:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
            raise

        Handler.active_codes.pop(token, None)
        self.send_html(page("Vot înregistrat", """
        <div class="card narrow center">
          <div style="font-size:60px;color:#067647">✓</div>
          <h1>Vot înregistrat</h1>
          <p class="muted">Votul a fost salvat cu succes.</p>
          <p class="muted">Codul nu mai poate fi folosit.</p>
        </div>
        """))


class ReusableThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def start_server(port):
    server = ReusableThreadingTCPServer(("0.0.0.0", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


if __name__ == "__main__":
    init_db()

    # Railway injectează de regulă PORT. Pentru compatibilitate maximă,
    # aplicația ascultă și pe porturile uzuale 5000 și 8080.
    requested_ports = []
    for candidate in (PORT, 5000, 8080):
        try:
            candidate = int(candidate)
        except (TypeError, ValueError):
            continue
        if 1 <= candidate <= 65535 and candidate not in requested_ports:
            requested_ports.append(candidate)

    servers = []
    active_ports = []
    for port in requested_ports:
        try:
            servers.append(start_server(port))
            active_ports.append(port)
        except OSError as exc:
            print(f"Nu pot deschide portul {port}: {exc}", flush=True)

    if not servers:
        raise RuntimeError("Aplicația nu a putut deschide niciun port HTTP.")

    public_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
    print("\n========================================", flush=True)
    print("APLICAȚIA DE VOTARE A PORNIT", flush=True)
    print("========================================", flush=True)
    print(f"Host: 0.0.0.0", flush=True)
    print(f"PORT din mediu: {os.environ.get('PORT', '(lipsește)')}", flush=True)
    print(f"Porturi active: {', '.join(map(str, active_ports))}", flush=True)
    if public_domain:
        print(f"Domeniu public: https://{public_domain}", flush=True)
    else:
        print(f"Local: http://127.0.0.1:{active_ports[0]}", flush=True)
    print(f"Parola administrator: {ADMIN_PASSWORD}", flush=True)
    print("========================================\n", flush=True)

    # Ținem procesul principal activ. Serverele HTTP rulează în fire daemon.
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
