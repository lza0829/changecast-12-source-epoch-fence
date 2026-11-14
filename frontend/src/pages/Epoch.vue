<script setup>
import { computed, onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const panel = ref(null)
const bypassRows = ref([])
const policies = ref({ late_epoch_policies: [], epoch_bump_buffer_policies: [] })
const busy = ref(false)
const err = ref('')
const msg = ref('')
const replayBusy = ref(null)

async function refresh() {
  panel.value = await getJSON('/api/epoch')
  bypassRows.value = await getJSON('/api/epoch/bypass')
}

onMounted(async () => {
  try {
    policies.value = await getJSON('/api/epoch/policies')
    await refresh()
  } catch (e) { err.value = String(e.message || e) }
})

const streams = computed(() => (panel.value ? panel.value.streams : []))

async function setPolicy(stream, key, value) {
  err.value = ''
  try {
    await postJSON(`/api/epoch/policies/${stream.id}`, { [key]: value })
    await refresh()
  } catch (e) { err.value = String(e.message || e) }
}

async function bump(stream, policy) {
  err.value = ''; msg.value = ''; busy.value = true
  try {
    const r = await postJSON('/api/epoch/bump', {
      stream_id: stream.id,
      buffer_policy: policy || stream.epoch_bump_buffer_policy,
    })
    msg.value = `流 ${stream.code}：epoch ${r.from_epoch} → ${r.to_epoch}；缓冲 ${r.buffered_seen} 条，移送旁路 ${r.buffered_migrated} 条，拒绝 ${r.buffered_rejected} 条。`
    await refresh()
  } catch (e) { err.value = String(e.message || e) } finally { busy.value = false }
}

async function seedBuffer(stream) {
  err.value = ''; msg.value = ''
  try {
    const r = await postJSON('/api/epoch/seed-buffer', { stream_id: stream.id, count: 2 })
    msg.value = `已在流 ${stream.code} 挂入 ${r.inserted} 条 epoch ${r.epoch} 的未完成缓冲。`
    await refresh()
  } catch (e) { err.value = String(e.message || e) }
}

async function replay(row, force) {
  err.value = ''; msg.value = ''; replayBusy.value = row.id
  try {
    const r = await postJSON('/api/epoch/replay', {
      stream_id: row.stream_id,
      bypass_id: row.id,
      allow_overwrite_newer: force,
    })
    if (r.result === 'blocked') {
      msg.value = `重放被拦：${r.entity_key} 已被新世代写过（${r.reason}）。如需强压，请用「强压重放」，日志会记为显式覆盖。`
    } else {
      msg.value = `重放入主投影：${r.event_id} → ${r.entity_key}${r.overwrote_existing ? '（覆盖既有行）' : ''}。`
    }
    await refresh()
  } catch (e) { err.value = String(e.message || e) } finally { replayBusy.value = null }
}
</script>

<template>
  <div class="page">
    <h1>世代围栏</h1>
    <p class="muted">
      投影流维护当前源 epoch。事件自带 epoch；小于当前流 epoch 的事件不改主投影与水位——
      按策略「拒绝并计数」或「进入旧世代旁路桶」。宣告提升时原子切换当前 epoch，并按策略处置旧 epoch 未完成缓冲。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <p v-if="msg" class="tag ok">{{ msg }}</p>

    <div class="panel" v-for="s in streams" :key="s.id">
      <h3 style="margin-top:0">
        {{ s.title }} <span class="mono muted">({{ s.code }})</span>
      </h3>
      <div class="stat-grid">
        <div class="stat"><div class="stat-num">{{ s.current_epoch }}</div><div class="stat-label">当前 epoch</div></div>
        <div class="stat"><div class="stat-num">{{ s.epoch_bump_count }}</div><div class="stat-label">提升次数</div></div>
        <div class="stat"><div class="stat-num">{{ s.epoch_rejected_count }}</div><div class="stat-label">旧世代拒绝</div></div>
        <div class="stat"><div class="stat-num">{{ s.epoch_bypassed_count }}</div><div class="stat-label">旁路入桶</div></div>
        <div class="stat"><div class="stat-num">{{ s.bypass_open }}</div><div class="stat-label">旁路待处理</div></div>
        <div class="stat"><div class="stat-num">{{ s.epoch_replayed_count }}</div><div class="stat-label">已重放</div></div>
        <div class="stat"><div class="stat-num">{{ s.epoch_replay_blocked_count }}</div><div class="stat-label">重放被拦</div></div>
        <div class="stat"><div class="stat-num">{{ s.watermark_seq }}</div><div class="stat-label">水位 seq</div></div>
        <div class="stat"><div class="stat-num">{{ s.fixture_stage }}</div><div class="stat-label">夹具阶段</div></div>
      </div>
      <p class="mono muted" style="margin-top:0.6rem">
        主投影行 {{ s.projection_rows }} = 存活 {{ s.projected_alive }} + 已删 {{ s.projected_deleted }}
        · 旁路行 {{ s.bypass_rows }} · 事件留痕 {{ s.applied_events }}
        <span v-if="s.last_epoch_bump_at"> · 最近提升 {{ s.last_epoch_bump_at }}</span>
      </p>

      <div style="display:flex;flex-wrap:wrap;gap:0.75rem;align-items:flex-end;margin-top:0.75rem">
        <label>
          旧世代策略
          <select :value="s.late_epoch_policy" @change="setPolicy(s, 'late_epoch_policy', $event.target.value)"
                  style="display:block;margin-top:0.25rem">
            <option v-for="p in policies.late_epoch_policies" :key="p" :value="p">
              {{ p === 'reject' ? '拒绝并计数' : '进入旁路桶' }}
            </option>
          </select>
        </label>
        <label>
          提升时缓冲处置
          <select :value="s.epoch_bump_buffer_policy"
                  @change="setPolicy(s, 'epoch_bump_buffer_policy', $event.target.value)"
                  style="display:block;margin-top:0.25rem">
            <option v-for="p in policies.epoch_bump_buffer_policies" :key="p" :value="p">
              {{ p === 'reject_buffer' ? '拒绝一切旧 epoch 缓冲' : '旧 epoch 缓冲迁入旁路' }}
            </option>
          </select>
        </label>
        <button :disabled="busy" @click="bump(s)">提升 epoch（{{ s.current_epoch }} → {{ s.current_epoch + 1 }}）</button>
        <button :disabled="busy" style="background:#5f6b78" @click="seedBuffer(s)">挂入旧世代缓冲（演示）</button>
      </div>
    </div>

    <div class="panel" v-if="panel">
      <h3 style="margin-top:0">旁路桶（可查、可手工重放，默认不写主投影）</h3>
      <p class="muted" style="margin-top:0">
        重放须走单独接口；若目标实体已被当前世代写过，重放默认被拦，须显式「强压重放」。
      </p>
      <table>
        <thead>
          <tr>
            <th>流</th><th>event_id</th><th>实体键</th><th>op</th><th>seq</th>
            <th>epoch</th><th>来源</th><th>状态</th><th>最近重放</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in bypassRows" :key="r.id">
            <td>{{ r.stream_code }}</td>
            <td class="mono">{{ r.event_id }}</td>
            <td class="mono">{{ r.entity_key }}</td>
            <td class="mono">{{ r.op }}</td>
            <td class="mono">{{ r.seq }}</td>
            <td class="mono">{{ r.epoch }}</td>
            <td>
              <span class="tag warn">{{ r.origin === 'bump_migrated' ? '提升迁入' : '迟到入桶' }}</span>
            </td>
            <td><span class="tag" :class="r.status === 'open' ? 'warn' : 'ok'">{{ r.status }}</span></td>
            <td class="mono">{{ r.last_replay_result || '—' }}</td>
            <td>
              <button :disabled="replayBusy === r.id || r.status === 'replayed'"
                      @click="replay(r, false)">{{ replayBusy === r.id ? '…' : '重放' }}</button>
              <button :disabled="replayBusy === r.id" style="background:#8f3d14" @click="replay(r, true)">
                强压重放
              </button>
            </td>
          </tr>
          <tr v-if="!bypassRows.length">
            <td colspan="10" class="muted">旁路桶为空（旧世代事件被拒绝，或尚未发生世代切换）。</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="panel" v-if="panel">
      <h3 style="margin-top:0">世代提升记录</h3>
      <table>
        <thead>
          <tr>
            <th>流</th><th>epoch</th><th>来源</th><th>缓冲处置</th>
            <th>缓冲 seen</th><th>拒绝</th><th>迁入旁路</th>
            <th>当时存活</th><th>累计拒绝</th><th>累计旁路</th><th>时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="c in panel.epoch_changes" :key="c.id">
            <td class="mono">{{ c.stream_id }}</td>
            <td class="mono">{{ c.from_epoch }} → {{ c.to_epoch }}</td>
            <td><span class="tag">{{ c.source === 'manual' ? '手动提升' : '夹具宣告' }}</span></td>
            <td class="mono">{{ c.buffer_policy }}</td>
            <td>{{ c.buffered_seen }}</td>
            <td>{{ c.buffered_rejected }}</td>
            <td>{{ c.buffered_migrated }}</td>
            <td>{{ c.projected_alive }}</td>
            <td>{{ c.epoch_rejected_total }}</td>
            <td>{{ c.epoch_bypassed_total }}</td>
            <td class="mono">{{ c.created_at }}</td>
          </tr>
          <tr v-if="!panel.epoch_changes.length">
            <td colspan="11" class="muted">尚无提升记录。</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="panel" v-if="panel">
      <h3 style="margin-top:0">旁路重放日志</h3>
      <table>
        <thead>
          <tr><th>流</th><th>event_id</th><th>实体键</th><th>结果</th><th>原因</th><th>时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="l in panel.replay_log" :key="l.id">
            <td class="mono">{{ l.stream_id }}</td>
            <td class="mono">{{ l.event_id }}</td>
            <td class="mono">{{ l.entity_key }}</td>
            <td><span class="tag" :class="l.result === 'blocked' ? 'danger' : 'ok'">{{ l.result }}</span></td>
            <td class="mono">{{ l.reason }}</td>
            <td class="mono">{{ l.created_at }}</td>
          </tr>
          <tr v-if="!panel.replay_log.length"><td colspan="6" class="muted">尚无重放。</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
