/**
 * config.js — 대시보드 API 연결 설정
 *
 * ngrok 재실행으로 URL이 바뀔 경우 이 파일의 API_BASE_URL만 수정하면 됩니다.
 * Vercel 배포 시에도 이 파일만 업데이트 후 재배포합니다.
 *
 * 로컬 테스트: API_BASE_URL = 'http://localhost:8000'
 * ngrok 배포:  API_BASE_URL = 'https://xxxx.ngrok-free.app'
 */

const CONFIG = {
  // ── 백엔드 서버 주소 ──────────────────────────────────────────────
  API_BASE_URL: '',   // ← 빈 문자열로 두면 현재 Vercel 도메인(또는 로컬 도메인)을 자동으로 사용합니다.

  // ── 인증 키 (.env의 DASHBOARD_SECRET_KEY와 동일하게 설정) ──────────
  API_KEY: 'DEV_KEY',                      // ← 운영 배포 시 강력한 키로 변경

  // ── 요청 헬퍼 ────────────────────────────────────────────────────
  async get(endpoint) {
    const res = await fetch(`${this.API_BASE_URL}${endpoint}`, {
      headers: { 'Authorization': `Bearer ${this.API_KEY}` },
    });
    if (!res.ok) throw new Error(`GET ${endpoint} 실패: ${res.status}`);
    return res.json();
  },

  async post(endpoint, body) {
    const res = await fetch(`${this.API_BASE_URL}${endpoint}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.API_KEY}`,
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`POST ${endpoint} 실패: ${res.status}`);
    return res.json();
  },
};
