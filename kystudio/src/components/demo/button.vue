<template>
<div>
<el-button>默认按钮</el-button>
<el-button type="primary">主要按钮</el-button>
<el-button type="text">文字按钮</el-button>
<br/>
<br/>
<el-button :plain="true" :disabled="true">主要按钮</el-button>
<el-button type="primary" :disabled="true">主要按钮</el-button>
<el-button type="text" :disabled="true">文字按钮</el-button>
<br/>
<br/>
<el-button type="primary" :loading="true">加载中</el-button>

<pre><code class="hljs language-html"><span class="hljs-tag">&lt;<span class="hljs-name">el-button</span>&gt;</span>默认按钮<span class="hljs-tag">&lt;/<span class="hljs-name">el-button</span>&gt;</span>
<span class="hljs-tag">&lt;<span class="hljs-name">el-button</span> <span class="hljs-attr">type</span>=<span class="hljs-string">"primary"</span>&gt;</span>主要按钮<span class="hljs-tag">&lt;/<span class="hljs-name">el-button</span>&gt;</span>
<span class="hljs-tag">&lt;<span class="hljs-name">el-button</span> <span class="hljs-attr">type</span>=<span class="hljs-string">"text"</span>&gt;</span>文字按钮<span class="hljs-tag">&lt;/<span class="hljs-name">el-button</span>&gt;</span>
</code></pre>

<pre><code class="hljs language-html"><span class="hljs-tag">&lt;<span class="hljs-name">el-button</span> <span class="hljs-attr">:plain</span>=<span class="hljs-string">"true"</span> <span class="hljs-attr">:disabled</span>=<span class="hljs-string">"true"</span>&gt;</span>主要按钮<span class="hljs-tag">&lt;/<span class="hljs-name">el-button</span>&gt;</span>
<span class="hljs-tag">&lt;<span class="hljs-name">el-button</span> <span class="hljs-attr">type</span>=<span class="hljs-string">"primary"</span> <span class="hljs-attr">:disabled</span>=<span class="hljs-string">"true"</span>&gt;</span>主要按钮<span class="hljs-tag">&lt;/<span class="hljs-name">el-button</span>&gt;</span>
<span class="hljs-tag">&lt;<span class="hljs-name">el-button</span> <span class="hljs-attr">type</span>=<span class="hljs-string">"text"</span> <span class="hljs-attr">:disabled</span>=<span class="hljs-string">"true"</span>&gt;</span>文字按钮<span class="hljs-tag">&lt;/<span class="hljs-name">el-button</span>&gt;</span>
</code></pre>


<pre><code class="hljs language-html"><span class="hljs-tag">&lt;<span class="hljs-name">el-button</span> <span class="hljs-attr">type</span>=<span class="hljs-string">"primary"</span> <span class="hljs-attr">:loading</span>=<span class="hljs-string">"true"</span>&gt;</span>加载中<span class="hljs-tag">&lt;/<span class="hljs-name">el-button</span>&gt;</span>
</code></pre>

</div>

</template>

<style scoped="">
	pre{
     	  padding:10px 10px;
     	  border:dashed 1px #ccc;
     	  margin:15px 5px;
    }
</style>
