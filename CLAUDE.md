# fsync — structures concurrentes rapides

Package `github.com/aytechnet/fsync` : alternatives concurrentes plus rapides que
`sync.Map` / `puzpuzpuz/xsync` pour la plateforme iPaaS DyaPi. Go 1.25.

Trois structures, trois niches :

| Type            | Clé             | Valeur (inline)            | Niche                                  |
|-----------------|------------------|----------------------------|----------------------------------------|
| `Store[V]`      | `int64`          | `[32]V` + `lockused`       | index entier dense, lock-free          |
| `MutexStore[V]` | `int64`          | `[64]V` + `[64]sync.Mutex` | idem, mutex par slot (contention forte) |
| `Map[K,V]`      | `K comparable`   | `[8]V` par bucket           | hash map générique concurrente         |

Différenciateur central : **`Lock(k)` renvoie un `*V` stable** vers la valeur
stockée *inline* dans le container. Pas d'allocation d'`*Entry`, pas
d'indirection — la cellule du bucket *est* l'entrée. Le pin survit aux
rebuilds (politique « duplicate-on-pin » du Map). Remplace l'idiome canonique
`map[K]*Entry{mu, v}`.

## Vue d'ensemble du code

| Fichier                          | Rôle                                            |
|----------------------------------|-------------------------------------------------|
| `store.go`                       | `Store[V]` + `MutexStore[V]`                    |
| `map.go`                         | `Map[K,V]` (le gros morceau, ~820 l.)            |
| `queue.go`                       | `Queue[T]` lock-free — historique, plus utilisé par `Map`/`Store` ; conservé pour usage applicatif |
| `store_test.go`                  | tests séquentiels + concurrents de Store/MutexStore + `LockOrStore` |
| `map_test.go`, `map_concur_test.go` | tests `Map` (séquentiel, rebuild, pins, concurrent) |
| `map_load_during_lock_test.go`   | test `Load` pendant `Lock` (build tag `!race`)  |
| `concur_test.go`                 | tests croisés                                   |
| `benchs/`                        | sous-package des benchmarks comparatifs         |
| `README.md`                      | description + tableaux de perf publiables       |

Versionné via git. Branche active : `wip/master-new`. Historiquement versionné
à la main via des snapshots `map.go.x*` / `store.go.x*` (toujours présents
dans l'arbre, gitignored, ne pas y toucher sans accord — ce sont les archives
manuelles de François).

## Contrat de concurrence (commun aux 3 structures)

Pour chaque clé indépendamment :

- `Load`, `Store`, `Delete` sont lock-free et O(1) moyen.
- `Lock(k)` épingle le slot exclusivement et renvoie un `*V` stable.
- `LockOrStore(k, v)` = insertion atomique + pin (`(*V, created bool)`).
- Tant qu'un slot est épinglé, **toute autre** `Load`/`Store`/`Delete` sur la
  **même clé** spin (`runtime.Gosched`) jusqu'au `Unlock`. Les autres clés,
  même dans le même bucket, ne sont pas bloquées.
- Le détenteur du `Lock` **DOIT** appeler `Unlock`. Pattern :
  `defer cur.Unlock()` juste après `Lock`.
- Le `*V` rendu par `Lock`/`LockOrStore` n'est valide que **jusqu'au
  `Unlock`** correspondant. Après, ne pas le déréférencer.

## `Store[V]` et `MutexStore[V]` (store.go)

Stockage dense indexé par entier : `index = i - start`.

### `Store[V]` — lock-free
- `bucketIndex = index >> 5`, `bi = index & 31`.
- `bucketStore[V] = struct { lockused atomic.Uint64; values [32]V }`.
  - bits 0..31 = `used`, bits 32..63 = `locked` (1 bit / slot).
- **`Lock` et `LockOrStore` : pattern Load-then-CAS** — Load shared sur
  `lockused`, si lockbit visible alors `runtime.Gosched()` (spin sans
  RMW, cacheline reste en Shared state), sinon CAS pour prendre le pin.
  Une seule CAS combine la prise du pin et la publication de `used=1`
  pour LockOrStore.
- `Unlock` : `And(^lockbit)`.
- **`Load`** : pattern historique `Or(lockbit)` + read + `And(^lockbit)`
  conservé volontairement. Une variante Load-then-CAS a été tentée
  (×24 sur single-key) puis revertée car elle régressait ReadHeavy de
  ~50 % (l'`Or` sur bits disjoints du même mot se fuse au niveau bus,
  ce que le CAS retry ne peut pas faire). Voir README "Design history".
- Trade-off : pour le pattern Lock+inc, `Store` gagne maintenant
  partout sauf sous contention **modérée** sur ~8 buckets (256 hot
  keys / 12 G), où `MutexStore` reste plus rapide grâce aux 64 mutex
  par cachelines disjointes. Sous contention **extrême** (single key
  pounded), `Store.Lock` (≈12 ns) bat `MutexStore.Lock` (≈51 ns)
  grâce à l'optim Load-then-CAS.

### `MutexStore[V]` — mutex par slot
- `bucketIndex = index >> 6`, `bi = index & 63`.
- `bucketMutexStore[V] = struct { used atomic.Uint64; mutexes [64]sync.Mutex; values [64]V }`.
- `LockOrStore` teste `used` **sous** le mutex pour éviter la race avec
  `Store` (qui publie `used` **après** avoir relâché le mutex).
- Plus performant que `Store` sous contention **modérée** (futex vs
  bit-spin sur 32 slots qui partagent une cacheline), moins performant
  sous contention extrême et sur les hot paths Load/Churn — bench dans
  le README.

### Croissance de la table (`bucketAlloc`)
La slice `buckets` croît en puissances de 2 avec recopie. Le couple
`table` / `newTable` sert de verrou : on CAS `newTable` pour réserver
pendant l'allocation, puis on rétablit. Les `bucketStore`/`bucketMutexStore`
**ne sont jamais déplacés** → adresses de `V` stables (garantie clé pour
`Lock`).

## `Map[K,V]` (map.go) — bucket-direct façon xsync

### Layout
```go
type bucket[K,V] struct {
    meta   atomic.Uint64   // 8 tags h7 packed (1 octet par slot)
    pins   atomic.Uint64   // 8 bits pin (bas) + 56 bits seq (haut)
    state  atomic.Uint32   // bucketOpen / bucketFrozen / bucketMoved
    mu     sync.Mutex      // sérialise les writers sur le bucket
    keys   [8]K
    values [8]V
    next   atomic.Pointer[bucket[K,V]]  // chaîne de débordement
}

type tableMap[K,V] struct {
    buckets     []atomic.Pointer[bucket[K,V]]
    mask        uint64
    rebuildLeft atomic.Int64   // buckets à migrer ; promotion quand ≤ 0
    rebuildIdx  atomic.Uint64  // prochain index à claim pour le sweep lazy
    nextTable   atomic.Pointer[tableMap[K,V]]
}

type Map[K,V] struct {
    seed  uint64
    live  atomic.Int64
    table atomic.Pointer[tableMap[K,V]]
}
```

### Hash
- `int / int64 / uint / uint64 / uintptr` : `hashUint64` = wyhash xsync-style
  (`Mul64(v^seed, PRIME64_1)`, xor hi/lo). ~10–20 fois plus rapide que
  `maphash` sur les entiers.
- Autres clés : `maphash.Comparable` (résistant au hash flooding).

### Tag h7
- `slotTag(h) = 0x80 | byte(h>>57)` : bit haut = occupé, 7 bits = signature.
- `meta` packe 8 tags → le hot-path `Load` scanne 8 slots avec un shift+mask
  sur un seul `Uint64`, soit ~1 cacheline lue.

### Pin / seq dans `pins atomic.Uint64`
- 8 bits pin (un par slot) en bas, compteur monotone 56 bits (seq) en haut.
- `Lock` / `Unlock` incrémentent `seq` en plus de toggler le bit.
- `Load` utilise le pattern seqlock : observe `pinsStart` (bit clair), lit
  `values[j]`, ré-observe `pinsEnd` ; égal ⇒ pas de Lock/Unlock pendant la
  lecture ⇒ pas de torn-read. Si le bit est positionné, `runtime.Gosched`
  puis retry.

### États de bucket
- `bucketOpen` (0) : accepte inserts/updates.
- `bucketFrozen` (1) : claimé par un migrateur (split classique en cours).
- `bucketMoved` (2) : migration finie ; lecteurs basculent sur `nextTable`.

### Politique de rebuild « split-or-duplicate »
Déclenchée quand `live > 8 * len(buckets) * 3/4` (load factor 0.75).

- **Split** (cas usuel) : bucket sans pin ⇒ deux nouveaux buckets dans
  `nextTable` selon le nouveau bit du masque. L'ancien passe `Moved`.
- **Duplicate** : bucket avec au moins un pin ⇒ **même pointeur** publié
  dans les deux entrées de `nextTable`. Le bucket reste vivant à son
  adresse d'origine ⇒ le `*V` rendu par `Lock` reste valide. Tag scan
  lit jusqu'aux 8 slots des deux côtés ; la comparaison de clé
  désambiguïse. État laissé à `Open` (pas `Moved` : les pins doivent
  pouvoir continuer à modifier).

### Sweep lazy + helping
- **Rebuild collaboratif** : chaque `Store`, `LockOrStore` (création) et
  `Lock` (création via overflow) migre `sweepBatch = 2` buckets via
  `helpRebuildProgress`, appelé depuis `afterInsert`. Les writers sont
  donc tous co-responsables de la progression du rebuild ; sous
  workload majoritairement read, le rebuild est piloté par les
  writers occasionnels.
- `Load` ne participe PAS au sweep (lock-free, n'incrémente pas
  `live`). C'est volontaire pour ne pas peser sur le hot path read.
- `Grow(N)` synchronise jusqu'à promotion totale, pour que la table
  attendue par le caller soit en place au retour.
- Cascading rebuilds protégés par `s.headTable.Load() != headTable`
  dans `maybeStartRebuild`.

### Layout bucket : pourquoi state n'est PAS fusionné avec pins (2026-06-19)
- La tentative de fusionner `state atomic.Uint32` (Open/Frozen/Moved)
  dans le mot `pins atomic.Uint64` (bits 8-9 du même mot, seq décalé
  à bits 10-63) a régressé Map.ReadOnly de +11 %, LoadOrStore de
  +14 % et StringReadOnly de +15 % — pour gagner −3 à −7 % sur
  Store/Churn/Lock+inc. Le mauvais trade-off vient probablement du
  changement de layout cacheline (4 octets de moins font glisser les
  arrays `[8]K [8]V`) et du `& pinsStateMask` supplémentaire sur le
  hot path Load.
- Le bucket conserve donc 4 atomiques séparées : `meta` (tags),
  `pins` (pin/seq seqlock), `state` (rebuild state), `mu` (sync.Mutex
  writer serialisation). Total ≈ 28 octets de "control" avant
  `[8]K [8]V [8]V next`.

### API publique
```go
NewMap[K,V](estimatedItems int) *Map[K,V]
(*Map[K,V]).Grow(estimatedItems int)
(*Map[K,V]).Load(k K) (V, bool)
(*Map[K,V]).Store(k K, v V) (created bool)
(*Map[K,V]).Delete(k K) bool
(*Map[K,V]).Lock(k K) (*V, Cursor[K,V], bool)
(*Map[K,V]).LockOrStore(k K, v V) (*V, Cursor[K,V], created bool)
(Cursor[K,V]).Unlock()                            // method on cursor
(*Map[K,V]).Len() int
// sync.Map-style atomic operations (no pin returned):
(*Map[K,V]).LoadOrStore(k K, v V) (actual V, loaded bool)
(*Map[K,V]).LoadAndDelete(k K) (V, bool)
(*Map[K,V]).Swap(k K, v V) (previous V, loaded bool)
(*Map[K,V]).CompareAndSwap(k K, old, new V) bool   // V interface-comparable
(*Map[K,V]).CompareAndDelete(k K, old V) bool       // V interface-comparable
(*Map[K,V]).Range(f func(K, V) bool)                // weakly consistent
(*Map[K,V]).Clear()
```

Les mêmes méthodes existent sur `Store[V]` et `MutexStore[V]` (avec
clé `int64`, signatures de `Lock`/`LockOrStore` retournant respectivement
`StoreCursor[V]` et `MutexStoreCursor[V]` au lieu de `Cursor[K,V]`).
`LoadOrStore` / `Swap` / `LoadAndDelete` / `CompareAndSwap` /
`CompareAndDelete` / `Range` / `Clear` ont la sémantique stricte
`sync.Map` (un `actual` pour `LoadOrStore`, pas de pin retourné, etc.).
`CompareAndSwap` et `CompareAndDelete` comparent via `any(*V) == any(old)`
— `V` doit être runtime-comparable (ni slice, ni map, ni func) sinon
panique à l'exécution.

Curseurs :
- `Cursor[K,V] = {bucket *bucket, slotIdx uint8}` (16 octets).
- `StoreCursor[V] = {store *Store[V], i int64}` (16 octets).
- `MutexStoreCursor[V] = {store *MutexStore[V], i int64}` (16 octets).

Chaque type a sa méthode `Unlock()` ; `cur.Unlock()` est l'idiome
attendu (le `(*Map[K,V]).Unlock(cursor)` / `(*Store[V]).Unlock(i)` /
`(*MutexStore[V]).Unlock(i)` ont été retirés). Le curseur du zero-value
(`Lock` qui n'a pas trouvé la clé) ne fait rien quand on l'`Unlock`.

`var m Map[K,V]` (zero-value) est utilisable : la première écriture
alloue lazyment une table de `firstSize = 64` buckets.

### Considérations sur le type V (à relire avant tout usage exotique)
- `V` de taille mot (`int`, `*T`, …) : lecture matériellement atomique
  amd64/arm64 ; seqlock couvre le Lock/Unlock concurrent ⇒ rien à faire.
- `V` multi-mots (struct, string, slice header) : la fenêtre de torn-read
  exige un Lock+modif+Unlock complet à l'intérieur d'un Load — rare. Si
  V est gros et que ça compte, le caller fait son Lock.
- `V` avec **état interne pointé** (`V = map[X]Y`, `V = *Sub`,
  `V = struct{ s []T }`) : le seqlock protège **uniquement le header de V**
  copié par Load. L'objet pointé n'est pas protégé. Soit on restreint
  l'accès à l'état interne aux détenteurs du Lock (pattern naturel puisque
  Load bloque sur Lock), soit on enrobe en `Map[K, *Sub]` avec `Sub`
  lui-même concurrence-safe.

## Tests

- `TestNewStore / TestNewMutexStore / TestStoreLockOrStore /
  TestMutexStoreLockOrStore` (store_test.go) : API + race séquentielle.
- `TestBigStore / TestBigMutexStore` : 100 k × GOMAXPROCS goroutines.
- `TestNewMap / TestStoreMap / TestBigMap` (map_test.go) : API + gros run
  concurrent.
- `TestMapLoadStore / TestMapLockUnlock / TestMapDelete /
  TestMapDeleteAndReinsert / TestMapLockOrStore / TestMapBigConcurrent /
  TestMapGrow / TestMapZeroValueUsable / TestMapRebuild /
  TestMapRebuildWithPins` (map_concur_test.go) : matrice concurrente.
- `TestMapLoadDuringLock` (`//go:build !race`) : isolation hors race
  detector (atomique mais non-atomique au sens Go memory model).

Tous tests verts : `go test -count=1 ./...` et `go test -race ./...`.

## Benchmarks (sous-package `./benchs/`)

5 fichiers : `fsync_bench_test.go`, `gomap_bench_test.go`,
`sync_bench_test.go`, `xsync_bench_test.go`, `mutexed_bench_test.go`.

5 workloads : `ReadOnly`, `ReadHeavy` (10:1), `Store`, `GrowStore`, `Churn`
(Store+Delete sur fenêtre roulante de 1024 clés). + `Lock+inc` pour les
patterns d'épinglage.

Reproduction (depuis la racine du package) :
```sh
go test -bench=. -benchtime=5s -count=3 -run='^$' ./benchs/
# variante 2s × 3 si la machine est limitée en RAM (sync.Map.Store OOM possible)
```

Tableaux complets et headline numbers dans `README.md`. Quelques chiffres
clés (Ryzen 5 8540U, médiane de 3 runs) :
- `fsync.Store.ReadOnly` : **0.75 ns/op** (plus rapide qu'`xsync.Map` à 1.02 ns).
- `fsync.Map.ReadOnly` : **1.44 ns/op** (~19 % au-dessus de `map[int]int` sans verrou).
- `fsync.MutexStore.LockOrStore+inc` : **4.80 ns/op** (vainqueur sur le pattern
  Lock+inc contendu, devant `xsync.Map[*{mu,v}]` à 7.40 ns).

## État actuel

- Branche unique : `master`. Worktrees / branches WIP intermédiaires
  ont été nettoyés. Préparation publication GitHub en cours :
  documentation à 100 %, couverture de tests 86.9 %, race detector
  clean, panic policy aligné (6 invariants internes silencés, 1
  panic d'erreur utilisateur gardé sur `cursor.Unlock` avec test
  dédié).
- Toutes les méthodes `sync.Map`-compatibles ajoutées sur les trois
  structures : `LoadOrStore`, `LoadAndDelete`, `Swap`,
  `CompareAndSwap`, `CompareAndDelete`, `Range`, `Clear`. Plus
  Store/MutexStore qui ont aussi `Lock` / `LockOrStore` / `Unlock`
  via cursors dédiés.
- Optimisations spin Store : `Lock` et `LockOrStore` réécrits en
  Load-then-CAS (×30 single-key contention) ; `Load` reverté après
  bench (gain pathologique mais régression sur ReadHeavy).

Tout est commit ; tests + race + benchs verts.

## Conventions

- **Lock / LockOrStore sur Store** : pattern Load-then-CAS. Load
  shared, spin via `runtime.Gosched()` si pin vu, CAS sinon.
- **Load / Store / Delete sur Store** : Or+And historique (le CAS
  perdrait des races sous contention typique).
- `Map` utilise un `sync.Mutex` par bucket pour sérialiser les
  writers (lecture lock-free).
- Tables grandissent en puissances de 2, jamais rétrécies, jamais
  déplacées au niveau bucket → adresses de `V` stables (garantie
  clé de `Lock`).
- **`panic` policy** : un seul panic dans `cursor.Unlock` (erreur
  utilisateur réelle : Unlock d'une slot déjà fully-zeroed via
  Delete intercalé). Les 6 anciens panics d'invariants internes
  ("table modified while being locked") ont été commentés —
  l'invariant reste documenté en ligne mais ne crash plus le
  programme client.
- **Commentaires dans le code souvent obsolètes** : héritages
  d'anciennes implémentations (deltas négatifs, layout iln, etc.).
  Se fier au code, pas aux commentaires.

## Rappel — PR awesome-go (≥ 2026-11-19)

PR awesome-go pour `fsync` à soumettre **à partir du 2026-11-19** (5 mois
après le premier commit, critère bloquant awesome-go).

- Matériel pré-rédigé : `~/Aytechnet/github/fsync-awesome-go-pr.md`
  (entrée exacte, rubrique cible `### Maps`, position alphabétique
  entre `dict` et `go-shelve`, PR body avec liens).
- Branche prête : `aytechnet/awesome-go` → `add-fsync` (commit
  `47eadc1`). À **rebaser sur `upstream/main`** au moment de la PR
  car la liste awesome-go bouge.
- Avant de soumettre, revérifier :
  - pkg.go.dev répond 200 sur
    https://pkg.go.dev/github.com/aytechnet/fsync ;
  - Go Report Card affiche toujours A+ ;
  - Codecov affiche la couverture à jour ;
  - position alphabétique encore correcte dans la rubrique Maps.
