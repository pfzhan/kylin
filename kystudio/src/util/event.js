export function stopPropagation (e) {
  e.stopPropagation()
  if (e.stopPropagation) {
    e.stopPropagation()
  }
  e.cancelBubble = true
}
