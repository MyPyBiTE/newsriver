(() => {
  "use strict";

  const FEEDS = {
    breaking: {
      filename: "breaking.json",
      listId: "breaking-list",
      statusId: "breaking-status",
      metaId: "breaking-meta"
    },

    newsriver: {
      filename: "newsriver.json",
      listId: "newsriver-list",
      statusId: "newsriver-status",
      metaId: "newsriver-meta"
    }
  };

  const DATA_ROOT =
    "../data/v2/";

  const CACHE_PREFIX =
    "mypybite-v2-last-known-good-";

  const TEST_MODE =
    new URLSearchParams(
      window.location.search
    ).get("test") || "";

  const SELECTION_MODES =
    new Set([
      "manual",
      "scheduled",
      "auto_fallback"
    ]);

  const ACCESS_TYPES =
    new Set([
      "FREE",
      "METERED",
      "PAYWALL",
      "UNKNOWN"
    ]);

  const TOP_LEVEL_FIELDS =
    new Set([
      "schema_version",
      "feed_id",
      "generated_at",
      "last_human_publish_at",
      "selection_mode",
      "stale_after_minutes",
      "count",
      "items"
    ]);

  const STORY_FIELDS =
    new Set([
      "story_id",
      "headline",
      "synopsis",
      "source",
      "url",
      "published_at",
      "category",
      "access_type",
      "selection_mode"
    ]);

  const PRIVATE_FIELDS =
    new Set([
      "editor_notes",
      "rights_note",
      "risk_class",
      "source_trust_tier",
      "auto_candidate",
      "auto_block_reason",
      "validation_state",
      "duplicate_of",
      "cluster_id",
      "source_story_id",
      "access_checked_at",
      "imported_at",
      "last_updated_at",
      "correction_of",
      "retraction_note"
    ]);

  function assert(
    condition,
    message
  ) {
    if (!condition) {
      throw new Error(message);
    }
  }

  function sameFields(
    value,
    allowed
  ) {
    const keys =
      Object.keys(value);

    return (
      keys.length ===
        allowed.size
      &&
      keys.every(
        (key) =>
          allowed.has(key)
      )
    );
  }

  function scanPrivateFields(
    value,
    path = "root"
  ) {
    if (Array.isArray(value)) {
      value.forEach(
        (child, index) =>
          scanPrivateFields(
            child,
            `${path}[${index}]`
          )
      );

      return;
    }

    if (
      !value
      ||
      typeof value !== "object"
    ) {
      return;
    }

    Object.entries(value)
      .forEach(
        ([key, child]) => {
          assert(
            !PRIVATE_FIELDS.has(key),
            (
              "Private field leaked at "
              + `${path}.${key}`
            )
          );

          scanPrivateFields(
            child,
            `${path}.${key}`
          );
        }
      );
  }

  function isIsoDate(value) {
    return (
      typeof value === "string"
      &&
      value.trim() !== ""
      &&
      Number.isFinite(
        Date.parse(value)
      )
    );
  }

  function isSafeUrl(value) {
    try {
      const parsed =
        new URL(value);

      return (
        parsed.protocol === "http:"
        ||
        parsed.protocol === "https:"
      );
    } catch (error) {
      return false;
    }
  }

  function wordCount(value) {
    return String(value || "")
      .trim()
      .split(/\s+/)
      .filter(Boolean)
      .length;
  }

  function validateStory(
    story,
    label
  ) {
    assert(
      story
      &&
      typeof story === "object"
      &&
      !Array.isArray(story),
      `${label} must be an object`
    );

    assert(
      sameFields(
        story,
        STORY_FIELDS
      ),
      (
        `${label} differs from `
        + "the public story contract"
      )
    );

    [
      "story_id",
      "headline",
      "synopsis",
      "source",
      "category"
    ].forEach(
      (field) => {
        assert(
          typeof story[field]
            === "string"
          &&
          story[field].trim()
            !== "",
          `${label}.${field} is required`
        );
      }
    );

    assert(
      wordCount(
        story.synopsis
      ) <= 25,
      (
        `${label}.synopsis `
        + "exceeds 25 words"
      )
    );

    assert(
      isSafeUrl(story.url),
      `${label}.url is invalid`
    );

    assert(
      isIsoDate(
        story.published_at
      ),
      (
        `${label}.published_at `
        + "is invalid"
      )
    );

    assert(
      ACCESS_TYPES.has(
        story.access_type
      ),
      (
        `${label}.access_type `
        + "is invalid"
      )
    );

    assert(
      SELECTION_MODES.has(
        story.selection_mode
      ),
      (
        `${label}.selection_mode `
        + "is invalid"
      )
    );
  }

  function validateFeed(
    feed,
    kind
  ) {
    assert(
      feed
      &&
      typeof feed === "object"
      &&
      !Array.isArray(feed),
      `${kind} feed must be an object`
    );

    scanPrivateFields(feed);

    assert(
      sameFields(
        feed,
        TOP_LEVEL_FIELDS
      ),
      (
        `${kind} feed differs from `
        + "the public contract"
      )
    );

    assert(
      feed.schema_version === 1,
      (
        `${kind}.schema_version `
        + "must be 1"
      )
    );

    assert(
      typeof feed.feed_id
        === "string"
      &&
      feed.feed_id.trim()
        !== "",
      `${kind}.feed_id is required`
    );

    assert(
      isIsoDate(
        feed.generated_at
      ),
      (
        `${kind}.generated_at `
        + "is invalid"
      )
    );

    assert(
      isIsoDate(
        feed.last_human_publish_at
      ),
      (
        `${kind}.last_human_publish_at `
        + "is invalid"
      )
    );

    assert(
      SELECTION_MODES.has(
        feed.selection_mode
      ),
      (
        `${kind}.selection_mode `
        + "is invalid"
      )
    );

    assert(
      Number.isInteger(
        feed.stale_after_minutes
      )
      &&
      feed.stale_after_minutes > 0,
      (
        `${kind}.stale_after_minutes `
        + "is invalid"
      )
    );

    assert(
      Number.isInteger(
        feed.count
      )
      &&
      feed.count >= 0,
      `${kind}.count is invalid`
    );

    assert(
      Array.isArray(
        feed.items
      ),
      `${kind}.items must be an array`
    );

    assert(
      feed.count
        === feed.items.length,
      (
        `${kind}.count does not `
        + "match items length"
      )
    );

    assert(
      kind !== "breaking"
      ||
      feed.items.length <= 6,
      "breaking feed exceeds 6 items"
    );

    const ids =
      new Set();

    feed.items.forEach(
      (story, index) => {
        validateStory(
          story,
          `${kind}.items[${index}]`
        );

        assert(
          !ids.has(
            story.story_id
          ),
          (
            `${kind} contains duplicate `
            + `story_id ${story.story_id}`
          )
        );

        ids.add(
          story.story_id
        );
      }
    );

    return feed;
  }

  function clone(value) {
    return JSON.parse(
      JSON.stringify(value)
    );
  }

  function applyTestMode(
    feed,
    kind
  ) {
    if (!TEST_MODE) {
      return feed;
    }

    const testFeed =
      clone(feed);

    if (
      TEST_MODE === "corrupt"
      &&
      kind === "newsriver"
    ) {
      testFeed.items = [
        {
          story_id: "BROKEN"
        }
      ];

      testFeed.count = 1;
    }

    if (
      TEST_MODE === "stale"
    ) {
      testFeed.generated_at =
        "2000-01-01T00:00:00Z";
    }

    if (
      TEST_MODE === "empty"
    ) {
      testFeed.items = [];
      testFeed.count = 0;
    }

    return testFeed;
  }

  function isStale(feed) {
    const generatedAt =
      Date.parse(
        feed.generated_at
      );

    const staleAt =
      generatedAt
      +
      (
        feed.stale_after_minutes
        * 60
        * 1000
      );

    return Date.now() > staleAt;
  }

  function readCache(kind) {
    try {
      const raw =
        localStorage.getItem(
          CACHE_PREFIX + kind
        );

      if (!raw) {
        return null;
      }

      return validateFeed(
        JSON.parse(raw),
        kind
      );
    } catch (error) {
      return null;
    }
  }

  function writeCache(
    kind,
    feed
  ) {
    if (TEST_MODE) {
      return;
    }

    try {
      localStorage.setItem(
        CACHE_PREFIX + kind,
        JSON.stringify(feed)
      );
    } catch (error) {
      /*
       * Live rendering still works
       * if browser storage is blocked.
       */
    }
  }

  async function loadFeed(
    kind,
    config
  ) {
    try {
      if (
        TEST_MODE === "offline"
      ) {
        throw new Error(
          "Simulated network failure"
        );
      }

      const response =
        await fetch(
          (
            DATA_ROOT
            + config.filename
            + `?v=${Date.now()}`
          ),
          {
            cache: "no-store"
          }
        );

      if (!response.ok) {
        throw new Error(
          `HTTP ${response.status}`
        );
      }

      const fetched =
        await response.json();

      const tested =
        applyTestMode(
          fetched,
          kind
        );

      const valid =
        validateFeed(
          tested,
          kind
        );

      writeCache(
        kind,
        valid
      );

      return {
        feed: valid,
        source: "live",
        stale: isStale(valid),
        error: null
      };
    } catch (error) {
      const cached =
        readCache(kind);

      if (cached) {
        return {
          feed: cached,
          source: "cache",
          stale: true,
          error
        };
      }

      return {
        feed: null,
        source: "unavailable",
        stale: true,
        error
      };
    }
  }

  function formatDate(value) {
    const date =
      new Date(value);

    if (
      !Number.isFinite(
        date.getTime()
      )
    ) {
      return "Unknown time";
    }

    return new Intl.DateTimeFormat(
      "en-CA",
      {
        dateStyle: "medium",
        timeStyle: "short"
      }
    ).format(date);
  }

  function createTextElement(
    tag,
    className,
    text
  ) {
    const element =
      document.createElement(tag);

    if (className) {
      element.className =
        className;
    }

    element.textContent =
      text;

    return element;
  }

  function createStoryCard(
    story,
    breaking
  ) {
    const article =
      document.createElement(
        "article"
      );

    article.className =
      "story-card";

    const content =
      document.createElement(
        "div"
      );

    const meta =
      createTextElement(
        "p",
        "story-meta",
        (
          `${story.category} · `
          + `${story.source} · `
          + formatDate(
              story.published_at
            )
        )
      );

    const title =
      document.createElement(
        "h3"
      );

    title.className =
      "story-title";

    const link =
      createTextElement(
        "a",
        "story-link",
        story.headline
      );

    link.href =
      story.url;

    link.target =
      "_blank";

    link.rel =
      "noopener noreferrer";

    title.appendChild(link);

    const synopsis =
      createTextElement(
        "p",
        "story-synopsis",
        story.synopsis
      );

    content.append(
      meta,
      title,
      synopsis
    );

    article.appendChild(
      content
    );

    const badgeText =
      (
        story.selection_mode
          .replace("_", " ")
        + " · "
        + story.access_type
      );

    const badge =
      createTextElement(
        "span",
        "badge",
        badgeText
      );

    if (breaking) {
      article.appendChild(
        badge
      );
    } else {
      content.insertBefore(
        badge,
        synopsis
      );
    }

    return article;
  }

  function setStatus(
    node,
    result
  ) {
    node.className =
      "status";

    if (!result.feed) {
      node.textContent =
        "Unavailable";

      node.classList.add(
        "status-unavailable"
      );

      return;
    }

    if (
      result.source === "cache"
    ) {
      node.textContent =
        "Cached fallback";

      node.classList.add(
        "status-cached"
      );

      return;
    }

    if (result.stale) {
      node.textContent =
        "Stale";

      node.classList.add(
        "status-stale"
      );

      return;
    }

    node.textContent =
      "Live";

    node.classList.add(
      "status-live"
    );
  }

  function renderFeed(
    kind,
    result,
    config
  ) {
    const list =
      document.getElementById(
        config.listId
      );

    const status =
      document.getElementById(
        config.statusId
      );

    const meta =
      document.getElementById(
        config.metaId
      );

    list.replaceChildren();

    setStatus(
      status,
      result
    );

    if (!result.feed) {
      meta.textContent =
        (
          "No valid public feed and no "
          + "last-known-good browser cache "
          + "are available."
        );

      list.appendChild(
        createTextElement(
          "p",
          "empty-state",
          "Feed unavailable."
        )
      );

      return;
    }

    const feed =
      result.feed;

    meta.textContent =
      (
        `Updated ${formatDate(
          feed.generated_at
        )} · `
        + `${feed.count} item`
        + (
          feed.count === 1
            ? ""
            : "s"
        )
        + ` · feed mode `
        + feed.selection_mode
      );

    if (!feed.items.length) {
      list.appendChild(
        createTextElement(
          "p",
          "empty-state",
          (
            "No stories are currently "
            + "available."
          )
        )
      );

      return;
    }

    feed.items.forEach(
      (story) => {
        list.appendChild(
          createStoryCard(
            story,
            kind === "breaking"
          )
        );
      }
    );
  }

  function showTestMode() {
    if (!TEST_MODE) {
      return;
    }

    const node =
      document.getElementById(
        "test-mode"
      );

    node.hidden = false;

    node.textContent =
      (
        `TEST MODE: `
        + TEST_MODE.toUpperCase()
        + " — fixture files are "
        + "not being modified."
      );
  }

  async function start() {
    showTestMode();

    const entries =
      Object.entries(FEEDS);

    const results =
      await Promise.all(
        entries.map(
          ([kind, config]) =>
            loadFeed(
              kind,
              config
            )
        )
      );

    entries.forEach(
      (
        [kind, config],
        index
      ) => {
        renderFeed(
          kind,
          results[index],
          config
        );
      }
    );
  }

  start();
})();
