import os
import requests
from flask import Flask, render_template_string, request, redirect, url_for, jsonify
from databricks import sql
from datetime import datetime

app = Flask(__name__)

TABLE = "chipgraham.schema1.mock_gold_alerts_table"

# Helper to get user email from Databricks SCIM /Me API
def get_user_email_from_token(token):
    try:
        db_host = os.environ.get("DATABRICKS_HOST")
        if not db_host or not token:
            return "unknown_user"
        resp = requests.get(
            f"https://{db_host}/api/2.0/preview/scim/v2/Me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5
        )
        if resp.status_code == 200:
            return resp.json().get("userName", "unknown_user")
    except Exception:
        pass
    return "unknown_user"

def get_connection(user_token):
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=user_token
    )

@app.route("/", methods=["GET"])
def index():
    user_token = request.headers.get("x-forwarded-access-token")
    if not user_token:
        return "User token not found. Make sure user authorization is enabled.", 401

    with get_connection(user_token) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT alert_key, window_start, acknowledged FROM {TABLE} ORDER BY window_start DESC")
            df = cursor.fetchall_arrow().to_pandas()

    return render_template_string('''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Alert Acknowledgement Table</title>
    <!-- Tailwind CSS CDN -->
    <script src="https://cdn.tailwindcss.com"></script>
    <!-- Alpine.js CDN -->
    <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>
</head>
<body class="bg-gray-50 min-h-screen flex flex-col">
    <!-- Navbar -->
    <nav class="bg-white shadow-md py-4 px-6 flex items-center justify-between">
        <div class="flex items-center space-x-2">
            <span class="text-2xl font-bold text-blue-700 tracking-tight">Alert Dashboard</span>
        </div>
    </nav>

    <!-- Main Content -->
    <main class="flex-1 flex flex-col items-center justify-start py-8 px-2 sm:px-0">
        <div class="w-full max-w-4xl">
            <div class="bg-white rounded-xl shadow-lg p-6">
                <h2 class="text-xl sm:text-2xl font-semibold text-gray-800 mb-6">Alert Acknowledgement Table</h2>
                <form id="ackForm" x-data="ackForm()" @submit.prevent="submitAck" class="space-y-4">
                    <div class="overflow-x-auto rounded-lg">
                        <table class="min-w-full divide-y divide-gray-200">
                            <thead class="bg-gray-100">
                                <tr>
                                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Select</th>
                                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Alert Key</th>
                                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Window Start</th>
                                    <th class="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Acknowledged?</th>
                                </tr>
                            </thead>
                            <tbody class="bg-white divide-y divide-gray-100">
                                {% for row in rows %}
                                <tr class="hover:bg-blue-50 transition-colors duration-150">
                                    <td class="px-4 py-2">
                                        {% if row.acknowledged == "N" %}
                                        <input type="checkbox" name="alert_keys" value="{{row.alert_key}}" class="accent-blue-600 w-4 h-4 rounded border-gray-300 focus:ring-blue-500">
                                        {% else %}<span class="text-gray-400">-</span>{% endif %}
                                    </td>
                                    <td class="px-4 py-2 font-mono text-sm text-gray-700">{{row.alert_key}}</td>
                                    <td class="px-4 py-2 text-gray-600">{{row.window_start}}</td>
                                    <td class="px-4 py-2">
                                        {% if row.acknowledged == "Y" %}
                                        <span class="inline-block px-2 py-1 text-xs font-semibold rounded bg-green-100 text-green-700">Yes</span>
                                        {% else %}
                                        <span class="inline-block px-2 py-1 text-xs font-semibold rounded bg-yellow-100 text-yellow-700">No</span>
                                        {% endif %}
                                    </td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                    <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4 mt-6">
                        <div x-show="error" x-transition class="text-red-600 text-sm font-medium">{{ error }}</div>
                        <button type="submit" :disabled="loading" class="inline-flex items-center justify-center px-6 py-2 bg-blue-600 text-white font-semibold rounded-lg shadow hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-offset-2 transition disabled:opacity-50 disabled:cursor-not-allowed">
                            <span x-show="!loading">Acknowledge Selected</span>
                            <svg x-show="loading" class="animate-spin h-5 w-5 text-white ml-2" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                                <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"></path>
                            </svg>
                        </button>
                    </div>
                </form>
            </div>
        </div>
    </main>
    <script>
    function ackForm() {
        return {
            loading: false,
            error: '',
            submitAck() {
                const form = document.getElementById('ackForm');
                const checkboxes = form.querySelectorAll('input[name="alert_keys"]:checked');
                const ids = Array.from(checkboxes).map(cb => cb.value);
                if (ids.length === 0) {
                    this.error = 'Please select at least one alert to acknowledge.';
                    return;
                }
                this.error = '';
                this.loading = true;
                fetch("{{url_for('acknowledge')}}", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        "x-forwarded-access-token": "{{request.headers.get('x-forwarded-access-token', '')}}"
                    },
                    body: JSON.stringify({alert_keys: ids})
                })
                .then(response => {
                    this.loading = false;
                    if (response.redirected) {
                        window.location.href = response.url;
                    } else {
                        window.location.reload();
                    }
                })
                .catch(() => {
                    this.loading = false;
                    this.error = 'An error occurred. Please try again.';
                });
            }
        }
    }
    </script>
</body>
</html>
''', rows=df.to_dict(orient="records"))

@app.route("/acknowledge", methods=["POST"])
def acknowledge():
    user_token = request.headers.get("x-forwarded-access-token")
    if not user_token:
        return "User token not found. Make sure user authorization is enabled.", 401

    # Get user email from Databricks SCIM /Me API
    user_email = get_user_email_from_token(user_token)

    if request.is_json:
        alert_keys = request.json.get("alert_keys", [])
    else:
        alert_keys = request.form.getlist("alert_keys")

    if not alert_keys:
        return "No row ids provided.", 400

    placeholders = ','.join(['?'] * len(alert_keys))
    now = datetime.utcnow().isoformat()

    with get_connection(user_token) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE {TABLE}
                SET acknowledged = 'Y',
                    acknowledged_by = ?,
                    acknowledged_at = ?
                WHERE alert_key IN ({placeholders})
                AND acknowledged = 'N'
                """,
                (user_email, now, *alert_keys)
            )

    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)