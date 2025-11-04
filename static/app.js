const $ = (sel) => document.querySelector(sel);

const API_BASE = "";

const api = {

  async getJob(jobId) {
    const clean = (jobId || "").trim().replace(/^"|"$/g, "").replace(/^'|'$/g, "");
    const res = await fetch(`${API_BASE}/jobs/${encodeURIComponent(clean)}`);
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  },

  async getTask(taskId) {
    const clean = (taskId || "").trim().replace(/^"|"$/g, "").replace(/^'|'$/g, "");
    const res = await fetch(`${API_BASE}/tasks/${encodeURIComponent(clean)}`);
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  },
};

function pretty(obj) {
  try {
    return JSON.stringify(obj, null, 2);
  } catch {
    return String(obj);
  }
}

function attachHandlers() {
  const form = $("#job-form");
  const submitResult = $("#submit-result");
  const tasksContainer = document.querySelector("#tasks-container");
  const addBtn = document.querySelector("#btn-add-task");
  let pollTimer = null;

  function createTaskRow(defaults = { type: "sleep", seconds: 3 }) {
    const row = document.createElement("div");
    row.className = "row task-row";
    row.innerHTML = `
      <label>Type</label>
      <select class="task-type">
        <option value="sleep">sleep</option>
      </select>
      <label>Seconds</label>
      <input class="task-seconds" type="number" min="1" value="${Number(defaults.seconds) || 3}" />
      <button class="btn-remove" type="button">Remove</button>
    `;
    row.querySelector('.task-type').value = defaults.type || 'sleep';
    row.querySelector('.btn-remove').addEventListener('click', () => row.remove());
    return row;
  }

  function collectTasks() {
    const rows = tasksContainer.querySelectorAll('.task-row');
    const tasks = [];
    rows.forEach((row) => {
      const type = row.querySelector('.task-type').value.trim();
      const seconds = Number(row.querySelector('.task-seconds').value);
      if (!type) return;
      const s = Number.isFinite(seconds) && seconds > 0 ? seconds : 1;
      tasks.push({ type, payload: { duration: s } });
    });
    return tasks;
  }

  // initial row
  tasksContainer.appendChild(createTaskRow());
  addBtn.addEventListener('click', () => tasksContainer.appendChild(createTaskRow()));

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    submitResult.textContent = "Submitting...";
    try {
      let metadata = {};
      const raw = $("#job-metadata").value.trim();
      if (raw) {
        try { metadata = JSON.parse(raw); } catch { throw new Error("Invalid metadata JSON"); }
      }
      const tasks = collectTasks();
      if (tasks.length === 0) throw new Error('Add at least one task');

      const res = await fetch(`${API_BASE}/ui/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ metadata, tasks }),
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      const ids = Array.isArray(data.task_ids) ? data.task_ids.join(', ') : 'n/a';
      submitResult.textContent = `Job submitted: ${data.job_id} | Task IDs: ${ids}`;
      $("#job-id-input").value = data.job_id;
    } catch (err) {
      submitResult.textContent = `Error: ${err.message || err}`;
    }
  });

  $("#btn-fetch-job").addEventListener("click", async () => {
    $("#job-json").textContent = "Loading...";
    try {
      const jobId = $("#job-id-input").value;
      const job = await api.getJob(jobId);
      $("#job-json").textContent = pretty(job);
    } catch (err) {
      $("#job-json").textContent = `Error: ${err.message || err}`;
    }
  });

  // polling removed per request

  $("#btn-fetch-task").addEventListener("click", async () => {
    $("#task-json").textContent = "Loading...";
    try {
      const taskId = $("#task-id-input").value;
      const task = await api.getTask(taskId);
      $("#task-json").textContent = pretty(task);
    } catch (err) {
      $("#task-json").textContent = `Error: ${err.message || err}`;
    }
  });
}

document.addEventListener("DOMContentLoaded", attachHandlers);


