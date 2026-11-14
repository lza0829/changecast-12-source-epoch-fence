<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const stats = ref(null)
const err = ref('')
onMounted(async () => {
  try { stats.value = await getJSON('/api/dashboard') } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>总览</h1>
    <p class="muted">变更日志投影：源 CDC 事件按序落到当前投影，世代围栏挡住旧世代迟到事件，批次计数与快照互证。</p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel" v-if="stats">
      <div class="stat-grid">
        <div class="stat"><div class="stat-num">{{ stats.stream_count }}</div><div class="stat-label">投影流</div></div>
        <div class="stat"><div class="stat-num">{{ stats.enabled_streams }}</div><div class="stat-label">已启用</div></div>
        <div class="stat"><div class="stat-num">{{ stats.batch_count }}</div><div class="stat-label">应用批次</div></div>
        <div class="stat"><div class="stat-num">{{ stats.projection_alive }}</div><div class="stat-label">存活投影</div></div>
        <div class="stat"><div class="stat-num">{{ stats.projection_deleted }}</div><div class="stat-label">已删投影</div></div>
        <div class="stat"><div class="stat-num">{{ stats.ooo_buffered }}</div><div class="stat-label">缓冲中</div></div>
        <div class="stat"><div class="stat-num">{{ stats.epoch_bump_total }}</div><div class="stat-label">世代提升</div></div>
        <div class="stat"><div class="stat-num">{{ stats.epoch_rejected_total }}</div><div class="stat-label">世代拒绝</div></div>
        <div class="stat"><div class="stat-num">{{ stats.epoch_bypassed_open }}</div><div class="stat-label">旁路待处理</div></div>
      </div>
      <p style="margin-top:1rem">默认应用序 <span class="tag">{{ stats.apply_order }}</span>（按事件 seq 升序）</p>
      <p><router-link to="/run">去投影台跑一趟 →</router-link> · <router-link to="/epoch">查看世代围栏 →</router-link></p>
    </div>
  </div>
</template>
