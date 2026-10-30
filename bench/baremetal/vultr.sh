#!/bin/sh
# Rents and returns a Vultr bare-metal machine for a round. Needs
# VULTR_API_KEY in the environment, which this never prints, and an SSH
# public key (SSH_PUBKEY, default ~/.ssh/id_ed25519.pub). State lives in
# STATE (default ~/.cache/netcode-bench), out of the repo.
#   vultr.sh plans | os | create <plan> <region> <os_id> | status | delete
# A round on vbm-6c-32gb (Xeon E-2286G) took ~20 minutes to provision and
# cost under $1 an hour. Delete the machine when done: it bills until then.
set -e
: "${VULTR_API_KEY:?set VULTR_API_KEY}"
STATE=${STATE:-$HOME/.cache/netcode-bench}
SSH_PUBKEY=${SSH_PUBKEY:-$HOME/.ssh/id_ed25519.pub}
mkdir -p "$STATE"
api() { curl -fsS -H "Authorization: Bearer $VULTR_API_KEY" -H "Content-Type: application/json" "$@"; }
B=https://api.vultr.com/v2
case "$1" in
  plans) # bare-metal plans, cheapest first, with the regions that have them
    api "$B/plans-metal?per_page=100" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const p=JSON.parse(s).plans_metal.sort((a,b)=>a.monthly_cost-b.monthly_cost);for(const x of p)console.log(x.id.padEnd(22),String(x.cpu_count).padStart(3)+"c",x.cpu_model.padEnd(28),"$"+x.monthly_cost+"/mo",(x.locations||[]).slice(0,8).join(","))})' ;;
  os)
    api "$B/os?per_page=500" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{for(const o of JSON.parse(s).os)if(/ubuntu 24\.04.*x64/i.test(o.name))console.log(o.id,o.name)})' ;;
  create) # create <plan> <region> <os_id>; a plan sold out in one region may be in another
    KEY=$(api -X POST "$B/ssh-keys" -d "{\"name\":\"netcode-bench\",\"ssh_key\":\"$(cat "$SSH_PUBKEY")\"}" | node -pe 'JSON.parse(require("fs").readFileSync(0)).ssh_key.id')
    echo "$KEY" > "$STATE/sshkey.id"
    # sshkey_id, not sshkey_ids: the API ignores the wrong name and the box comes up without the key
    api -X POST "$B/bare-metals" -d "{\"region\":\"$3\",\"plan\":\"$2\",\"os_id\":$4,\"sshkey_id\":[\"$KEY\"],\"label\":\"netcode-bench\",\"hostname\":\"netcode-bench\"}" \
      | node -pe 'const b=JSON.parse(require("fs").readFileSync(0)).bare_metal; require("fs").writeFileSync(process.argv[1]+"/box.id", b.id); b.id+" "+b.status' "$STATE" ;;
  status)
    api "$B/bare-metals/$(cat "$STATE/box.id")" | node -pe 'const b=JSON.parse(require("fs").readFileSync(0)).bare_metal; [b.status, b.server_status||"", b.main_ip, b.plan, b.region].join(" ")' ;;
  delete)
    [ -f "$STATE/box.id" ] && api -X DELETE "$B/bare-metals/$(cat "$STATE/box.id")" && echo "box deleted" && rm "$STATE/box.id"
    [ -f "$STATE/sshkey.id" ] && api -X DELETE "$B/ssh-keys/$(cat "$STATE/sshkey.id")" && echo "ssh key deleted" && rm "$STATE/sshkey.id" ;;
esac
