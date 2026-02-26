const form = document.getElementById("uploadForm");
const fileInput = document.getElementById("fileInput");
const dropzone = document.getElementById("dropzone");
const clearBtn = document.getElementById("clearFile");
const statusEl = document.getElementById("status");
const submitBtn = document.getElementById("submitBtn");
const bankSelect = document.getElementById("bankSelect");
const fileNameEl = document.getElementById("fileName");
const copyMainBtn = document.getElementById("copyMainBtn");

const emailInput = document.getElementById("emailInput");
const passwordInput = document.getElementById("passwordInput");
const amountInput = document.getElementById("amountInput");
const registerBtn = document.getElementById("registerBtn");
const loginBtn = document.getElementById("loginBtn");
const logoutBtn = document.getElementById("logoutBtn");
const payBtn = document.getElementById("payBtn");
const authStatusEl = document.getElementById("authStatus");
const paymentStatusEl = document.getElementById("paymentStatus");

const API_BASE = window.APP_CONFIG?.API_BASE ?? window.location.origin;
const API_URL_SINGLE = `${API_BASE}/convert`;
const API_URL_MERGED = `${API_BASE}/convert-merge`;
const API_URL_REGISTER = `${API_BASE}/auth/register`;
const API_URL_LOGIN = `${API_BASE}/auth/login`;
const API_URL_ME = `${API_BASE}/auth/me`;
const API_URL_LOGOUT = `${API_BASE}/auth/logout`;
const API_URL_PAY = `${API_BASE}/billing/pay`;
const API_URL_MAIN_FILE = `${API_BASE}/main-file`;

let authState = {
  token: localStorage.getItem("extracto_token") || "",
  email: "",
  isPaid: false,
};

function setStatus(msg, type = "") {
  if (!statusEl) return;
  statusEl.textContent = msg;
  statusEl.className = "status " + type;
}

function setAuthStatus(msg, type = "") {
  if (!authStatusEl) return;
  authStatusEl.textContent = msg;
  authStatusEl.className = "status " + type;
}

function setPaymentStatus(msg, type = "") {
  if (!paymentStatusEl) return;
  paymentStatusEl.textContent = msg;
  paymentStatusEl.className = "status " + type;
}

function getMode() {
  const el = document.querySelector('input[name="downloadMode"]:checked');
  return el ? el.value : "single";
}

function selectedFiles() {
  return fileInput?.files ? Array.from(fileInput.files) : [];
}

function setSelectedFiles(files) {
  const dt = new DataTransfer();
  files.forEach((f) => dt.items.add(f));
  fileInput.files = dt.files;
  renderSelectedFiles();
}

function isPdf(file) {
  if (!file) return false;
  return file.type === "application/pdf" || file.name.toLowerCase().endsWith(".pdf");
}

function validatePDFs(files) {
  if (!files || !files.length) return "Seleccioná al menos un PDF.";
  const bad = files.find((f) => !isPdf(f));
  if (bad) return `Solo PDFs (.pdf). Archivo inválido: ${bad.name}`;
  return null;
}

function renderSelectedFiles() {
  if (!fileNameEl) return;
  const files = selectedFiles();
  if (!files.length) {
    fileNameEl.textContent = "";
    return;
  }
  if (files.length === 1) {
    fileNameEl.textContent = `Archivo seleccionado: ${files[0].name}`;
  } else {
    fileNameEl.textContent = `${files.length} archivos seleccionados: ` + files.map((f) => f.name).join(" · ");
  }
}

function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function getCredentials() {
  return {
    email: emailInput?.value?.trim?.() || "",
    password: passwordInput?.value || "",
  };
}

function validateCredentials() {
  const { email, password } = getCredentials();
  if (!email) return "Ingresá tu email.";
  if (!password || password.length < 6) return "La contraseña debe tener al menos 6 caracteres.";
  return null;
}

function authHeaders(extra = {}) {
  const headers = { ...extra };
  if (authState.token) {
    headers.Authorization = `Bearer ${authState.token}`;
  }
  return headers;
}

function saveToken(token) {
  authState.token = token || "";
  if (authState.token) {
    localStorage.setItem("extracto_token", authState.token);
  } else {
    localStorage.removeItem("extracto_token");
  }
}

async function readJsonOrThrow(res) {
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const detail = body?.detail || "Error del servidor";
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return body;
}

async function refreshSession() {
  if (!authState.token) {
    authState.email = "";
    authState.isPaid = false;
    setAuthStatus("No hay sesión activa.", "");
    return;
  }

  try {
    const res = await fetch(API_URL_ME, { headers: authHeaders() });
    const data = await readJsonOrThrow(res);
    authState.email = data.email || "";
    authState.isPaid = Boolean(data.is_paid);
    const paidText = authState.isPaid ? "✅ Cuenta habilitada por pago" : "⚠️ Falta pago para convertir";
    setAuthStatus(`Sesión activa: ${authState.email}. ${paidText}`, authState.isPaid ? "ok" : "");
  } catch {
    saveToken("");
    authState.email = "";
    authState.isPaid = false;
    setAuthStatus("No hay sesión activa.", "");
  }
}

if (dropzone) {
  dropzone.addEventListener("dragover", (e) => {
    e.preventDefault();
    dropzone.classList.add("dragover");
  });

  dropzone.addEventListener("dragleave", () => {
    dropzone.classList.remove("dragover");
  });

  dropzone.addEventListener("drop", (e) => {
    e.preventDefault();
    dropzone.classList.remove("dragover");

    const files = Array.from(e.dataTransfer.files || []);
    const onlyPdfs = files.filter(isPdf);

    const err = validatePDFs(onlyPdfs);
    if (err) {
      fileInput.value = "";
      renderSelectedFiles();
      setStatus(err, "err");
      return;
    }

    setSelectedFiles(onlyPdfs);
    setStatus(`Listo ✅ ${onlyPdfs.length} PDF(s) cargado(s).`, "ok");
  });
}

if (fileInput) {
  fileInput.addEventListener("change", () => {
    const files = selectedFiles();
    const err = validatePDFs(files);
    if (err) {
      fileInput.value = "";
      renderSelectedFiles();
      setStatus(err, "err");
      return;
    }
    renderSelectedFiles();
    setStatus(`Listo ✅ ${files.length} PDF(s) cargado(s).`, "ok");
  });
}

if (clearBtn) {
  clearBtn.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    fileInput.value = "";
    renderSelectedFiles();
    setStatus("Archivos quitados.", "");
  });
}

if (registerBtn) {
  registerBtn.addEventListener("click", async () => {
    const validationError = validateCredentials();
    if (validationError) {
      setAuthStatus(validationError, "err");
      return;
    }

    registerBtn.disabled = true;
    try {
      const res = await fetch(API_URL_REGISTER, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(getCredentials()),
      });
      const data = await readJsonOrThrow(res);
      setAuthStatus(data.message || "Usuario creado.", "ok");
    } catch (error) {
      setAuthStatus(`Error al crear usuario: ${error.message}`, "err");
    } finally {
      registerBtn.disabled = false;
    }
  });
}

if (loginBtn) {
  loginBtn.addEventListener("click", async () => {
    const validationError = validateCredentials();
    if (validationError) {
      setAuthStatus(validationError, "err");
      return;
    }

    loginBtn.disabled = true;
    try {
      const res = await fetch(API_URL_LOGIN, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(getCredentials()),
      });
      const data = await readJsonOrThrow(res);

      saveToken(data.token);
      authState.email = data.email;
      authState.isPaid = Boolean(data.is_paid);

      const payMsg = authState.isPaid
        ? "Cuenta habilitada para convertir."
        : "Iniciaste sesión. Falta pago para convertir.";
      setAuthStatus(`Sesión iniciada como ${authState.email}. ${payMsg}`, authState.isPaid ? "ok" : "");
    } catch (error) {
      setAuthStatus(`Error al iniciar sesión: ${error.message}`, "err");
    } finally {
      loginBtn.disabled = false;
    }
  });
}

if (logoutBtn) {
  logoutBtn.addEventListener("click", async () => {
    if (!authState.token) {
      setAuthStatus("No hay sesión para cerrar.", "");
      return;
    }

    logoutBtn.disabled = true;
    try {
      await fetch(API_URL_LOGOUT, {
        method: "POST",
        headers: authHeaders(),
      });
    } finally {
      saveToken("");
      authState.email = "";
      authState.isPaid = false;
      setAuthStatus("Sesión cerrada.", "ok");
      setPaymentStatus("", "");
      logoutBtn.disabled = false;
    }
  });
}

if (payBtn) {
  payBtn.addEventListener("click", async () => {
    if (!authState.token) {
      setPaymentStatus("Iniciá sesión antes de pagar.", "err");
      return;
    }

    const amount = Number(amountInput?.value);
    if (!Number.isFinite(amount) || amount <= 0) {
      setPaymentStatus("Ingresá un monto válido mayor que 0.", "err");
      return;
    }

    payBtn.disabled = true;
    try {
      const res = await fetch(API_URL_PAY, {
        method: "POST",
        headers: authHeaders({ "Content-Type": "application/json" }),
        body: JSON.stringify({ amount }),
      });
      const data = await readJsonOrThrow(res);
      authState.isPaid = true;
      setPaymentStatus(`${data.message} Monto: USD ${amount.toFixed(2)}.`, "ok");
      setAuthStatus(`Sesión activa: ${authState.email}. ✅ Cuenta habilitada por pago`, "ok");
    } catch (error) {
      setPaymentStatus(`Error en pago: ${error.message}`, "err");
    } finally {
      payBtn.disabled = false;
    }
  });
}

async function copyMainFileToClipboard() {
  if (!navigator?.clipboard?.writeText) {
    throw new Error("Tu navegador no permite copiar automáticamente.");
  }

  const res = await fetch(API_URL_MAIN_FILE);
  if (!res.ok) {
    throw new Error("No se pudo leer main.py desde el servidor.");
  }

  const content = await res.text();
  await navigator.clipboard.writeText(content);
}

async function fetchBlobOrThrow(res) {
  if (res.ok) return await res.blob();

  const contentType = (res.headers.get("content-type") || "").toLowerCase();
  let message = "";

  if (contentType.includes("application/json")) {
    const txt = await res.text().catch(() => "");
    if (txt) {
      try {
        const data = JSON.parse(txt);
        const detail = data?.detail;
        if (detail) {
          message = typeof detail === "string" ? detail : JSON.stringify(detail);
        } else {
          message = typeof data === "string" ? data : JSON.stringify(data);
        }
      } catch {
        message = txt;
      }
    }
  } else {
    message = await res.text().catch(() => "");
  }

  throw new Error(message || `Error del servidor (${res.status})`);
}

if (form) {
  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    if (!authState.token) {
      setStatus("Primero creá usuario e iniciá sesión.", "err");
      return;
    }

    if (!authState.isPaid) {
      setStatus("Tu cuenta todavía no está habilitada. Realizá el pago primero.", "err");
      return;
    }

    const files = selectedFiles();
    const err = validatePDFs(files);
    if (err) {
      setStatus(err, "err");
      return;
    }

    const mode = getMode();
    const bank = bankSelect?.value;

    submitBtn.disabled = true;

    try {
      if (mode === "single") {
        setStatus(`Procesando ${files.length} archivo(s)…`, "");

        for (let idx = 0; idx < files.length; idx++) {
          const f = files[idx];
          setStatus(`Procesando (${idx + 1}/${files.length}): ${f.name}`, "");

          const fd = new FormData();
          fd.append("file", f);
          fd.append("bank", bank);

          const res = await fetch(API_URL_SINGLE, { method: "POST", body: fd, headers: authHeaders() });
          const blob = await fetchBlobOrThrow(res);

          const outName = f.name.replace(/\.pdf$/i, "") + ".xlsx";
          downloadBlob(blob, outName);
        }

        setStatus("Listo ✅ Descargados todos.", "ok");
        return;
      }

      setStatus("Subiendo y unificando…", "");

      const fd = new FormData();
      fd.append("bank", bank);
      files.forEach((f) => fd.append("files", f));

      const res = await fetch(API_URL_MERGED, { method: "POST", body: fd, headers: authHeaders() });
      const blob = await fetchBlobOrThrow(res);

      const outName = `extractos_${bank}_unificado.xlsx`;
      downloadBlob(blob, outName);

      setStatus("Listo ✅ Excel unificado descargado.", "ok");
    } catch (error) {
      setStatus("Error: " + (error?.message || "Failed to fetch"), "err");
    } finally {
      submitBtn.disabled = false;
    }
  });
}

refreshSession();

if (copyMainBtn) {
  copyMainBtn.addEventListener("click", async () => {
    copyMainBtn.disabled = true;
    try {
      await copyMainFileToClipboard();
      setStatus("Listo ✅ main.py copiado al portapapeles.", "ok");
    } catch (error) {
      setStatus("Error al copiar main.py: " + (error?.message || "desconocido"), "err");
    } finally {
      copyMainBtn.disabled = false;
    }
  });
}

