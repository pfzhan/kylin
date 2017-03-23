<template>
      <li class="parent_li" v-if="expression !== 'TOP_N' && expression !== 'EXTENDED_COLUMN'">
        Value:<b>{{ nextpara.value }}</b>, 
        Type:<b>{{ nextpara.type }}</b>      
      </li>
      <parameterTree  v-else-if="nextpara.next_parameter!=null" :expression="expression" :nextpara="nextpara.next_parameter"></parameterTree>
</template>

<script>
export default {
  name: 'parameterTree',
  props: ['expression', 'nextpara'],
  data: function () {
    return {

    }
  },
  computed: {
    getNextPara: function () {
      this.nextpara
    }
  }
}
</script>
<style scoped="">

</style>
