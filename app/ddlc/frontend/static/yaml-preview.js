/**
 * YAMLPreview — Fetches and displays syntax-highlighted ODCS YAML.
 */
const YAMLPreview = {
    async load(sessionId) {
        const el = document.getElementById('yamlOutput');
        if (!el) return;
        el.textContent = 'Loading...';

        try {
            const resp = await fetch(`/api/sessions/${sessionId}/contract/yaml`);
            if (!resp.ok) throw new Error('Failed to load YAML');
            const yaml = await resp.text();
            el.innerHTML = this.highlight(yaml);
        } catch (err) {
            el.textContent = `Error: ${err.message}`;
        }
    },

    highlight(yaml) {
        // Simple regex-based YAML syntax highlighting
        return yaml.split('\n').map(line => {
            // Comment lines
            if (line.trim().startsWith('#')) {
                return `<span class="yaml-comment">${this.esc(line)}</span>`;
            }

            // Key-value lines
            const kvMatch = line.match(/^(\s*)([\w-]+)(\s*:\s*)(.*)/);
            if (kvMatch) {
                const [, indent, key, colon, value] = kvMatch;
                const highlightedValue = this.highlightValue(value);
                return `${indent}<span class="yaml-key">${this.esc(key)}</span>${colon}${highlightedValue}`;
            }

            // List items
            const listMatch = line.match(/^(\s*-\s*)(.*)/);
            if (listMatch) {
                const [, prefix, value] = listMatch;
                // Check if list item is a key-value
                const innerKV = value.match(/^([\w-]+)(\s*:\s*)(.*)/);
                if (innerKV) {
                    const [, key, colon, val] = innerKV;
                    return `${prefix}<span class="yaml-key">${this.esc(key)}</span>${colon}${this.highlightValue(val)}`;
                }
                return `${prefix}${this.highlightValue(value)}`;
            }

            return this.esc(line);
        }).join('\n');
    },

    highlightValue(value) {
        if (!value || !value.trim()) return value;

        const trimmed = value.trim();

        // Boolean
        if (/^(true|false)$/i.test(trimmed)) {
            return `<span class="yaml-bool">${this.esc(value)}</span>`;
        }

        // Null
        if (/^(null|~)$/i.test(trimmed)) {
            return `<span class="yaml-null">${this.esc(value)}</span>`;
        }

        // Number
        if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
            return `<span class="yaml-number">${this.esc(value)}</span>`;
        }

        // Quoted string
        if ((trimmed.startsWith("'") && trimmed.endsWith("'")) ||
            (trimmed.startsWith('"') && trimmed.endsWith('"'))) {
            return `<span class="yaml-string">${this.esc(value)}</span>`;
        }

        // Unquoted string
        if (trimmed.length > 0) {
            return `<span class="yaml-string">${this.esc(value)}</span>`;
        }

        return this.esc(value);
    },

    esc(str) {
        if (!str) return '';
        return str
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    },
};
