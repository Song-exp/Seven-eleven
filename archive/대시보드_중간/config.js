// config.js
// 프론트엔드 API 베이스 URL 설정.
// 시연 시 노트북의 실제 IP 또는 ngrok URL로 BASE를 교체하세요.

window.CONFIG = (function () {
  // [활성] 로컬 개발
  const BASE = 'http://localhost:8000';

  // [비활성] 시연용 외부 노출 URL (ngrok 또는 고정 IP)
  // const BASE = 'https://xxxx.ngrok-free.app';

  async function get(path) {
    const res = await fetch(BASE + path);
    if (!res.ok) throw new Error(`GET ${path} → ${res.status}`);
    return res.json();
  }

  async function post(path, body) {
    const res = await fetch(BASE + path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`POST ${path} → ${res.status}`);
    return res.json();
  }

  return { BASE, get, post };
})();
