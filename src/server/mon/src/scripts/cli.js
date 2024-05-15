const PROMPT_PREFIX = 'dragonfly>';
const API_URL = "/api";

function formatCommands(commands) {
    return commands[0].split(" ");
}

async function executeCommands(dbid, pre, input, commands, animate) {
    try {
        const reply = await execute(commands, dbid);
        for (const [i, command] of commands.entries()) {
            await writeLines(pre, input, command, formatReply(reply), animate, false);
        }
    } catch (err) {
        for (const command of commands) {
            await writeLines(pre, input, command, err.message, animate);
        }
    }
}

function formatReply(reply, indent = '') {
    console.log(Object.keys(reply));
    if (reply === null) {
        return '(nil)';
    } else {
        try {
            const rsp = reply;
            if (rsp.error) return rsp.error;
            if (rsp.result) return rsp.result;
            return JSON.stringify(rsp);
        } catch (error) {
            return error.message;
        }
    }
}

const autocomplete = {};
function initAutocomplete(element) {
    autocomplete.element = element;
}

function positionAutocomplete() {
    var rect = autocomplete.element.parentElement.getBoundingClientRect();
    autocomplete.element.style.left = rect.left + 100 + 'px';
    autocomplete.element.style.top = rect.top + 20 + 'px';
}

async function updateAutoComplete(text) {
    if (text && text.length) {
        const commands = await fetchCommands();
        const matches = commands.filter((c) => c.startsWith(text.toUpperCase())).filter((value, index, array) => array.indexOf(value) === index).sort((a, b) => a.localeCompare(b));

        if ((matches.length == 1) && (matches[0] == text)) {
            autocomplete.element.style.visibility = "hidden";
            return;
        }

        if (matches.length) {
            autocomplete.matches = matches;
            autocomplete.element.style.visibility = "visible";
            autocomplete.element.textContent = '';
            matches.forEach(m => {
                const promtDiv = document.createElement("div");
                promtDiv.className = "ac-prompt"
                promtDiv.textContent = m;
                autocomplete.element.appendChild(promtDiv);
            });
            positionAutocomplete();
            return;
        }
    }
    autocomplete.element.style.visibility = "hidden";
}

function applyAutocomplete(input) {
    if (autocomplete.matches && autocomplete.matches.length) {
        setInputValue(input, autocomplete.matches[0]);
        updateAutoComplete(autocomplete.matches[0]);
    }
}

async function createCli(cli) {
    const toExecute = getCommandsToExecute(cli);
    cli.replaceChildren();

    const pre = createPre(cli),
        [input, prompt] = createPrompt(cli),
        dbid = cli.getAttribute('dbid');

    drawTerminal(cli);
    handleHistory(pre, input);

    input.addEventListener('keydown', event => {
        if (event.key === 'Enter') {
            event.preventDefault();
            const command = input.value;
            input.value = '';
            updateAutoComplete('');
            if (!command.trim()) {
                return;
            }
            disablePrompt(cli, input, prompt, () => executeInputCommand(dbid, pre, input, command));
        }
        if (event.key === "Tab") {
            event.preventDefault();
            applyAutocomplete(input);
            return;
        }
    });

    input.addEventListener('input', e => {
        updateAutoComplete(e.target.value);
    })

    if (toExecute) {
        disablePrompt(cli, input, prompt, () =>
            executeCommands(dbid, pre, input, toExecute, shouldAnimate(cli)));
    }

}

function drawBadge(cli) {
    if (shouldAnimate(cli)) {
        return
    }
    const badge = document.createElement('div');
    badge.classList.add('powered');
    badge.appendChild(document.createTextNode('Powered by'));
    cli.appendChild(badge);
}

function drawTerminal(cli) {
    if (!isTerminal(cli)) return;
    const bar = document.createElement('div');
    bar.classList.add('bar');

    const buttons = ['#d00', '#0d0', '#00d'];
    buttons.forEach((b) => {
        let button = document.createElement('span');
        button.classList.add('button')
        // button.style.backgroundColor = b;
        bar.appendChild(button);
    });

    cli.classList.add('terminal');
    cli.prepend(bar);
}

function isTerminal(cli) {
    return cli.getAttribute('terminal') !== null
}

function shouldAnimate(cli) {
    try {
        return cli.getAttribute('typewriter') !== null &&
            !window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    } catch {
        return true;
    }
}

function getCommandsToExecute(cli) {
    const textContent = cli.textContent.trim();
    if (!textContent) return;

    return textContent.split('\n').map(x => x.trim());
}

function createPre(cli) {
    const pre = document.createElement('pre');
    pre.setAttribute('tabindex', '0');
    cli.appendChild(pre);
    return pre;
}

function createPrompt(cli) {
    const prompt = document.createElement('div');
    prompt.classList.add('prompt');

    const prefix = document.createElement('span');
    prefix.appendChild(document.createTextNode(PROMPT_PREFIX));
    prompt.appendChild(prefix);

    const input = document.createElement('input');
    input.setAttribute('name', 'prompt');
    input.setAttribute('type', 'text');
    input.setAttribute('autocomplete', 'off');
    input.setAttribute('spellcheck', 'false');
    prompt.appendChild(input);

    const autocomplete = document.createElement('div');
    autocomplete.classList.add('autocomplete');
    autocomplete.setAttribute("id", "autocomplete");
    prompt.appendChild(autocomplete);

    initAutocomplete(autocomplete);

    cli.appendChild(prompt);

    cli.addEventListener('click', () => {
        if (document.getSelection().type === 'Range') return;
        input.focus();
    });

    cli.addEventListener('keydown', event => {
        if (event.target === input) return;
        if (event.ctrlKey || event.altKey || event.shiftKey || event.metaKey) return;
        input.focus();
        input.scrollIntoView({ block: "nearest" });
    });
    return [input, prompt];
}

async function disablePrompt(cli, input, prompt, fn) {
    cli.classList.add('disabled');
    input.disabled = true;
    prompt.style.display = 'none';
    const p = Promise.all([fn()])
        .then(() => {
            prompt.style.display = '';
            cli.classList.remove('disabled');
            input.disabled = false;
            input.focus({ preventScroll: true });
        });
}

function handleHistory(pre, input) {
    let position = 0,
        tempValue = '';
    input.addEventListener('keydown', event => {
        switch (event.key) {
            case 'ArrowUp':
                event.preventDefault();

                if (position === Math.floor(pre.childNodes.length / 2)) return;
                else if (position === 0) tempValue = input.value;

                ++position;
                break;

            case 'ArrowDown':
                event.preventDefault();

                if (position === 0) return;
                else if (--position === 0) {
                    setInputValue(input, tempValue);
                    return;
                }
                break;

            default:
                return;
        }

        const { nodeValue } = pre.childNodes[pre.childNodes.length - position * 2];
        setInputValue(input, nodeValue.substring(PROMPT_PREFIX.length, nodeValue.length - 1));
    });
}

function setInputValue(input, value) {
    input.value = value;
    input.setSelectionRange(value.length, value.length);
}

async function writeLines(pre, input, command, reply, animate) {
    await writeLine(pre, input, command, animate, true);
    await writeLine(pre, input, reply, false, false);
}

async function executeInputCommand(dbid, pre, input, command) {
    switch (command.toLowerCase()) {
        case 'clear':
            pre.replaceChildren();
            break;

        /*
        case 'help':
            writeLine(pre, input, command, false, false);
            writeLine(pre, input, 'No problem!', false, false);
            break;
        */

        default:
            executeCommands(dbid, pre, input, [command]);
            break;
    }
}

let id;

async function execute(commands, dbid = '') {
    const response = await fetch(API_URL, {
        method: 'POST',
        mode: 'cors',
        cache: 'no-cache',
        credentials: 'same-origin',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(formatCommands(commands))
    });

    const reply = await response.json();
    id = reply.id;
    return reply;
}

async function writeLine(pre, input, line, animate, prompt) {
    const textNode = document.createTextNode('');
    pre.appendChild(textNode);

    const toWrite = line + '\n';
    if (prompt) textNode.nodeValue = PROMPT_PREFIX;
    if (!animate) {
        textNode.nodeValue += toWrite;
    } else {
        await typewriter(textNode, toWrite);
    }
    input.scrollIntoView({ block: "nearest" });

    var div = document.getElementById("terminal");
    setTimeout(() => div.scrollTop = div.scrollHeight, 10);
}

function typewriter(textNode, toWrite) {
    return new Promise(resolve => {
        let i = 0;
        const intervalId = setInterval(() => {
            if (i === toWrite.length) {
                clearInterval(intervalId);
                resolve();
                return;
            }

            textNode.nodeValue += toWrite[i++];
        }, 10 + Math.random() * 25);
    });
}

document.addEventListener('DOMContentLoaded', () => {
    for (const cli of document.querySelectorAll('.terminal')) {
        createCli(cli);
    }
});
