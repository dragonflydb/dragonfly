
async function loadAchievementsTab() {
    try {
        const response = await fetch("achievements.html");
        if (!response.ok) return;
        const html = await response.text();
        const container = document.getElementById('achievements');
        container.innerHTML = html;

        const scripts = container.querySelectorAll('script');
        scripts.forEach(script => {
            const newScript = document.createElement('script');
            if (script.src) {
                newScript.src = script.src;
            } else {
                newScript.textContent = script.textContent;
            }
            document.body.appendChild(newScript);
            document.body.removeChild(newScript);
        });
    } catch (e) { }
}