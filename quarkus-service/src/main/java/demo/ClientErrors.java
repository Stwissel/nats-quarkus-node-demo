package demo;

/**
 * Decides what a failure is allowed to tell the HTTP client.
 *
 * <p>Exists because that decision was previously split in two. {@link
 * JsonExceptionMapper} redacted 5xx messages, but the explicit {@code catch}
 * blocks in {@link ApiResource} built their 502s directly and never went through
 * it — so the broker URL the mapper was added to hide still reached the browser.
 * Both paths now call in here.
 */
final class ClientErrors {

  private ClientErrors() {}

  /** Replaces {@code user:password@} credentials in any embedded URL with {@code ***@}. */
  static String redact(String text) {
    return text == null ? null : text.replaceAll("(?<=://)[^/@\\s]+:[^/@\\s]+@", "***@");
  }

  /** The message a client may be shown for a failure it caused. */
  static String forClient(Throwable ex) {
    return redact(ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage());
  }
}
