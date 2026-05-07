async function initDashboard() {
    const stats = await (await fetch('/api/grid_stats')).json();
    const demand = await (await fetch('/api/demand_trends')).json();
    const infra = await (await fetch('/api/infrastructure_hotspots')).json();

    document.getElementById('out-savings').innerText = stats.total_savings;
    document.getElementById('out-peak').innerText = stats.peak_reduction;
    document.getElementById('out-alerts').innerText = stats.active_alerts;
    document.getElementById('out-energy').innerText = stats.total_energy.toLocaleString() + " kWh";

    const hours = [...new Set(demand.map(d => d.Hour))].sort((a,b)=>a-b);
    const ctx = document.getElementById('mainChart').getContext('2d');
    
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: hours.map(h => h + ":00"),
            datasets: [
                {
                    label: 'Unmanaged Load (Baseline)',
                    data: hours.map(h => demand.filter(d => d.Hour === h).reduce((s,i)=>s+i.Energy_Requested_kWh, 0)),
                    borderColor: '#dc3545',
                    borderDash: [5, 5],
                    fill: false
                },
                {
                    label: 'TechAZsure Optimized Load',
                    data: hours.map(h => demand.filter(d => d.Hour === h).reduce((s,i)=>s+i.Optimized_Load, 0)),
                    borderColor: '#0dcaf0',
                    backgroundColor: 'rgba(13, 202, 240, 0.1)',
                    fill: true,
                    tension: 0.4
                }
            ]
        },
        options: { responsive: true, maintainAspectRatio: false }
    });

    const table = document.getElementById('infra-table');
    infra.forEach(item => {
        table.innerHTML += `
            <tr>
                <td>${item.Zone}</td>
                <td><code>${item.Transformer_ID}</code></td>
                <td><div class="progress" style="height: 8px;"><div class="progress-bar bg-info" style="width: ${item.GHI}%"></div></div></td>
                <td><span class="badge bg-light text-dark">Priority ${item.GHI < 60 ? 'High' : 'Medium'}</span></td>
                <td class="text-success fw-bold">+ ₹${Math.round(item.Savings_INR / 1000)}k</td>
            </tr>`;
    });
}
initDashboard();