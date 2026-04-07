// Auto-dismiss alertas de sucesso após 5s
document.addEventListener('DOMContentLoaded', () => {
  setTimeout(() => {
    document.querySelectorAll('.alert-success').forEach(el =>
      bootstrap.Alert.getOrCreateInstance(el)?.close()
    );
  }, 5000);
});
