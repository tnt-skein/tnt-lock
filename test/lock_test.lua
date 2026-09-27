--- Тесты кластерного замка: захват, продление, потеря и отпускание.
---
--- Хранилище решений подменяется двойником: проверяется поведение замка,
--- а не поведение etcd.

local t = require('luatest')
local json = require('json')

local g = t.group('tnt.lock')

local helper = dofile('test/helper.lua')

---@type any
local lock

---@type any
local client

---@type any
local state

--- Который час по мнению замка.
---@type number
local now

--- Срок аренды: остальные сроки считаются от него.
local TTL = 60

g.before_each(function()
    now = 1000

    lock = helper.load(helper.MODULES, 'tnt.lock')

    client, state = helper.store()

    lock._set_source({
        monotonic = function()
            return now
        end,

        realtime = function()
            return 1789041300
        end,
    })

    lock.configure({ client = client, identity = 'storage-001-a', ttl = TTL })
end)

g.after_each(function()
    lock._set_source(nil)
    helper.unload(helper.MODULES)
end)

g.test_free_lock_is_taken = function()
    local taken, err = lock.acquire('snapshot')

    t.assert_equals(taken, true)
    t.assert_equals(err, nil)
    t.assert_equals(lock.holds('snapshot'), true)
end

g.test_owner_is_written_down = function()
    -- По записи видно, кто держит замок и с какого времени: без этого
    -- оператору остаётся гадать, чьей работы он ждёт.
    lock.acquire('snapshot')

    local owner = lock.owner('snapshot')

    t.assert_equals(owner.identity, 'storage-001-a')
    t.assert_equals(owner.operation, 'snapshot')
    t.assert_equals(owner.since, 1789041300)
end

g.test_busy_lock_is_refused = function()
    -- Ключ создаётся с условием «его ещё нет»: занявший первым держит
    -- замок, остальные уходят ни с чем.
    state.keys['/app/locks/snapshot'] = { value = json.encode({ identity = 'сосед' }), revision = 1 }

    local taken, err = lock.acquire('snapshot')

    t.assert_equals(taken, false)
    t.assert_str_contains(err, 'замок занят')
    t.assert_equals(lock.holds('snapshot'), false)
end

g.test_storage_failure_is_not_a_free_lock = function()
    -- «Занят другим» и «до хранилища не достучаться» выглядят одинаково
    -- только для невнимательного кода: держатель, возможно, жив.
    state.behaviour.lease_grant_error = 'хранилище молчит'

    local taken, err = lock.acquire('snapshot')

    t.assert_equals(taken, false)
    t.assert_str_contains(err, 'аренда не взята')
end

g.test_lost_race_returns_its_lease = function()
    -- Аренда берётся до попытки занять ключ: проигравший обязан отозвать
    -- её, иначе она висит до конца срока и занимает место в хранилище.
    state.keys['/app/locks/snapshot'] = { value = '{}', revision = 1 }

    lock.acquire('snapshot')

    local revoked = false

    for _, call in ipairs(state.calls) do
        if call.op == 'lease_revoke' then
            revoked = true
        end
    end

    t.assert_equals(revoked, true)
end

g.test_same_node_takes_its_lock_again_without_a_second_lease = function()
    lock.acquire('snapshot')

    local leases = 0

    -- Повторное взятие своего замка — удача, а не отказ: вызывающий
    -- судит по ответу, и `nil` отправил бы его ждать собственный замок.
    local taken, err = lock.acquire('snapshot')

    t.assert_equals(taken, true)
    t.assert_equals(err, nil)

    for _, call in ipairs(state.calls) do
        if call.op == 'lease_grant' then
            leases = leases + 1
        end
    end

    t.assert_equals(leases, 1, 'вторая аренда не бралась')
end

g.test_lock_is_released = function()
    lock.acquire('snapshot')

    t.assert_equals(lock.release('snapshot'), true)

    t.assert_equals(lock.holds('snapshot'), false)
    t.assert_equals(state.keys['/app/locks/snapshot'], nil)
end

g.test_releasing_a_lock_that_is_not_held_changes_nothing = function()
    t.assert_equals(lock.release('snapshot'), false)
end

g.test_renewal_confirms_the_hold = function()
    lock.acquire('snapshot')
    now = now + 30

    t.assert_equals(lock.renew('snapshot'), true)

    now = now + 30

    t.assert_equals(lock.holds('snapshot'), true, 'подтверждение отодвинуло срок')
end

g.test_hold_expires_without_renewal = function()
    -- Срок держания — аренда за вычетом запаса: узел узнаёт о потере
    -- замка раньше хранилища, а не позже.
    lock.acquire('snapshot')
    now = now + TTL - TTL / 3

    t.assert_equals(lock.holds('snapshot'), false)
end

g.test_expired_lease_is_forgotten = function()
    lock.acquire('snapshot')

    state.behaviour.lease_keepalive_error = { category = 'LEASE_EXPIRED', message = 'истекла' }

    local renewed, err = lock.renew('snapshot')

    t.assert_equals(renewed, false)
    t.assert_str_contains(err, 'аренда истекла')
    t.assert_equals(lock.holds('snapshot'), false, 'держаться больше не за что')
end

g.test_failed_renewal_does_not_take_the_lock_away = function()
    -- Право держать замок отмеряется сроком, а не успехом одного запроса.
    lock.acquire('snapshot')

    state.behaviour.lease_keepalive_error = 'хранилище молчит'

    local renewed, err = lock.renew('snapshot')

    t.assert_equals(renewed, false)
    t.assert_str_contains(err, 'аренда не продлена')
    t.assert_equals(lock.holds('snapshot'), true)
end

g.test_renewing_a_lock_that_is_not_held_says_so = function()
    local renewed, err = lock.renew('snapshot')

    t.assert_equals(renewed, false)
    t.assert_str_contains(err, 'замок не взят')
end

g.test_all_locks_are_renewed_at_once = function()
    -- Такт продления один на узел: операций у него бывает несколько,
    -- а аренда у каждой своя.
    lock.acquire('snapshot')
    lock.acquire('reclaim')

    now = now + 30

    t.assert_equals(lock.renew_all(), {})

    now = now + 30

    t.assert_equals(lock.holds('snapshot'), true)
    t.assert_equals(lock.holds('reclaim'), true)
end

g.test_lost_locks_are_named = function()
    lock.acquire('snapshot')
    lock.acquire('reclaim')

    state.behaviour.lease_keepalive_error = { category = 'LEASE_EXPIRED', message = 'истекла' }

    t.assert_equals(lock.renew_all(), { 'reclaim', 'snapshot' })
end

g.test_guarded_body_runs_and_releases = function()
    local answer = lock.guarded('snapshot', function()
        return 'снято'
    end)

    t.assert_equals(answer, 'снято')
    t.assert_equals(lock.holds('snapshot'), false, 'замок отпущен')
    t.assert_equals(state.keys['/app/locks/snapshot'], nil)
end

g.test_guarded_body_is_skipped_when_the_lock_is_busy = function()
    state.keys['/app/locks/snapshot'] = { value = '{}', revision = 1 }

    local ran = false
    local answer, err = lock.guarded('snapshot', function()
        ran = true
    end)

    t.assert_equals(ran, false)
    t.assert_equals(answer, nil)
    t.assert_str_contains(err, 'замок занят')
end

g.test_guarded_body_releases_the_lock_even_when_it_falls = function()
    -- Операция, уронившая узел в исключение, не должна оставлять кластер
    -- без обслуживания до конца аренды.
    local ok, err = pcall(lock.guarded, 'snapshot', function()
        error('снимок не задался', 0)
    end)

    -- Исключение выходит тем же, каким его бросило тело: приписка места
    -- внутри замка увела бы человека искать поломку не там.
    t.assert_equals(ok, false)
    t.assert_equals(err, 'снимок не задался')

    t.assert_equals(lock.holds('snapshot'), false)
    t.assert_equals(state.keys['/app/locks/snapshot'], nil)
end

g.test_owner_of_a_free_lock_is_nobody = function()
    t.assert_equals(lock.owner('snapshot'), nil)
end

g.test_unreadable_record_is_not_an_owner = function()
    -- Запись в замке кто-то переписал руками: разобрать её нельзя,
    -- и выдумывать держателя по обломкам незачем.
    state.keys['/app/locks/snapshot'] = { value = 'не json', revision = 1 }

    local owner, err = lock.owner('snapshot')

    t.assert_equals(owner, nil)
    t.assert_str_contains(err, 'не разобрана')
end

g.test_record_that_is_not_a_record_has_no_owner = function()
    -- Разобралось, но не в то: в ключе лежит число, а держатель — это
    -- запись с именем узла. Отдать число наружу значит уронить того,
    -- кто спросит у держателя имя.
    state.keys['/app/locks/snapshot'] = { value = '42', revision = 1 }

    local owner, err = lock.owner('snapshot')

    t.assert_equals(owner, nil)
    t.assert_str_contains(err, 'не разобрана')
end

g.test_storage_failure_hides_the_owner = function()
    state.behaviour.get_error = 'хранилище молчит'

    local owner, err = lock.owner('snapshot')

    t.assert_equals(owner, nil)
    t.assert_str_contains(err, 'молчит')
end

g.test_lock_needs_a_client = function()
    t.assert_error_msg_contains('клиент хранилища', lock.configure, {})
end

g.test_prefix_and_identity_fall_back_to_defaults = function()
    lock.configure({ client = client })
    lock.acquire('snapshot')

    t.assert_not_equals(state.keys['/app/locks/snapshot'], nil)
    t.assert_equals(lock.owner('snapshot').identity, 'неизвестный')
end

g.test_prefix_is_chosen_by_configuration = function()
    lock.configure({ client = client, prefix = '/tnt/locks' })
    lock.acquire('snapshot')

    t.assert_not_equals(state.keys['/tnt/locks/snapshot'], nil)
end

g.test_occupier_takes_and_releases = function()
    -- Тот же договор, что и у местной занятости узла: взяли — вернули
    -- способ отпустить.
    local release, err = lock.occupy('snapshot')

    t.assert_type(release, 'function')
    t.assert_equals(err, nil)
    t.assert_equals(lock.holds('snapshot'), true)

    release()

    t.assert_equals(lock.holds('snapshot'), false)
end

g.test_occupier_names_the_reason = function()
    state.keys['/app/locks/snapshot'] = { value = '{}', revision = 1 }

    local release, err = lock.occupy('snapshot')

    t.assert_equals(release, nil)
    t.assert_str_contains(err, 'замок занят')
end

g.test_keeper_renews_while_it_runs = function()
    -- Без продления замок живёт ровно одну аренду: чекпойнт на большой
    -- арене идёт дольше.
    lock.configure({ client = client, identity = 'storage-001-a', ttl = 0.15 })
    lock.acquire('snapshot')
    lock.start()

    local running = lock.running()

    require('fiber').sleep(0.3)

    local renewals = 0

    for _, call in ipairs(state.calls) do
        if call.op == 'lease_keepalive' then
            renewals = renewals + 1
        end
    end

    lock.stop()

    t.assert_equals(running, true)
    t.assert_gt(renewals, 0)
    t.assert_equals(lock.running(), false)
end

g.test_lost_lock_is_written_down = function()
    local journal = helper.capture_log()

    journal.forget()

    lock.configure({ client = client, identity = 'storage-001-a', ttl = 0.15 })
    lock.acquire('snapshot')

    state.behaviour.lease_keepalive_error = { category = 'LEASE_EXPIRED', message = 'истекла' }

    lock.start()
    require('fiber').sleep(0.2)
    lock.stop()

    t.assert_equals(journal.logged('замок потерян'), true)
end

g.test_broken_keeper_tick_is_reported = function()
    local journal = helper.capture_log()

    journal.forget()

    lock.configure({ client = client, identity = 'storage-001-a', ttl = 0.15 })
    lock.acquire('snapshot')

    lock._set_source({
        monotonic = function()
            error('часы встали')
        end,
    })

    lock.start()
    require('fiber').sleep(0.2)
    lock.stop()

    t.assert_equals(journal.logged('продление замков'), true)
end

g.test_stale_hold_is_dropped_before_a_new_attempt = function()
    -- Срок вышел, а аренда всё ещё числится за нами: хранилище к этому
    -- времени уже отдало замок, и держаться за прежнюю аренду значит
    -- не взять его заново никогда.
    lock.acquire('snapshot')

    local key = '/app/locks/snapshot'

    -- Хранилище отпустило ключ по истечении аренды, а узел об этом
    -- ещё не знает.
    state.keys[key] = nil
    now = now + TTL

    t.assert_equals(lock.acquire('snapshot'), true)
    t.assert_not_equals(state.keys[key], nil)

    local leases = 0

    for _, call in ipairs(state.calls) do
        if call.op == 'lease_grant' then
            leases = leases + 1
        end
    end

    t.assert_equals(leases, 2, 'вторая аренда взята заново')
end

g.test_unexplained_refusal_is_passed_on = function()
    -- Отказ хранилища бывает и не про занятость: причина уходит
    -- оператору как есть, потому что придумать её лучше него мы не можем.
    state.behaviour.txn_create_error = 'шлюз ответил пятисоткой'

    local taken, err = lock.acquire('snapshot')

    t.assert_equals(taken, false)
    t.assert_str_contains(err, 'замок не взят')
    t.assert_str_contains(err, 'пятисоткой')
end

g.test_settings_fall_back_to_defaults = function()
    lock.configure({ client = client })

    t.assert_equals(lock.status().ttl, 60)
    t.assert_equals(lock.status().renew_interval, 20)
end

g.test_lease_is_taken_for_the_configured_time = function()
    -- Срок аренды едет в хранилище: ошибка в нём видна только там.
    lock.configure({ client = client })
    lock.acquire('snapshot')

    local granted

    for _, call in ipairs(state.calls) do
        if call.op == 'lease_grant' then
            granted = call.ttl
        end
    end

    t.assert_equals(granted, 60)
end

g.test_status_names_what_is_held = function()
    lock.acquire('snapshot')
    lock.acquire('reclaim')

    local status = lock.status()

    t.assert_equals(status.held, { 'reclaim', 'snapshot' })
    t.assert_equals(status.identity, 'storage-001-a')
    t.assert_equals(status.prefix, '/app/locks')
    t.assert_equals(status.running, false)
end

g.test_temporary_failure_does_not_count_as_a_lost_lock = function()
    -- Право держать замок отмеряется сроком: одно непрошедшее продление
    -- ещё ничего не отнимает, и объявлять замок потерянным рано.
    lock.acquire('snapshot')

    state.behaviour.lease_keepalive_error = 'хранилище молчит'

    t.assert_equals(lock.renew_all(), {})
    t.assert_equals(lock.holds('snapshot'), true)
end

g.test_stale_lease_is_revoked_before_the_new_one = function()
    -- Прежняя аренда отзывается: иначе она висит до конца срока
    -- и без нужды занимает место в хранилище.
    lock.acquire('snapshot')

    state.keys['/app/locks/snapshot'] = nil
    state.calls = {}
    now = now + TTL

    lock.acquire('snapshot')

    local revoked = 0

    for _, call in ipairs(state.calls) do
        if call.op == 'lease_revoke' then
            revoked = revoked + 1
        end
    end

    t.assert_equals(revoked, 1)
end

g.test_missing_client_is_blamed_on_the_caller = function()
    -- Настройку пишет программист: бросок называет его строку, а не строку
    -- внутри замка, где чинить нечего.
    local err, place = helper.refusal(function()
        lock.configure({ ttl = TTL })
    end)

    t.assert_equals(err, place .. ': замку нужен клиент хранилища решений')
end

g.test_renewal_step_as_long_as_the_lease_is_refused = function()
    -- Шаг не меньше срока не оставляет узлу срока держания: замок числился
    -- бы своим вечно, а хранилище отдало бы его другому между продлениями.
    local err, place = helper.refusal(function()
        lock.configure({ client = client, ttl = 30, renew_interval = 30 })
    end)

    t.assert_equals(
        err,
        place
            .. ': замку нужен положительный срок аренды и шаг продления меньше него, а не срок 30 и шаг 30'
    )

    -- Отвергнутая настройка ничего не поменяла: замок живёт с прежней.
    t.assert_equals(lock.status().ttl, TTL)
    t.assert_equals(lock.status().renew_interval, TTL / 3)
end

g.test_zero_lease_is_refused = function()
    local err = helper.refusal(function()
        lock.configure({ client = client, ttl = 0 })
    end)

    t.assert_str_contains(err, 'а не срок 0 и шаг 0')
end

g.test_renewal_step_just_under_the_lease_is_accepted = function()
    -- Граница: шаг на секунду короче срока — узел держит замок секунду
    -- после подтверждения.
    lock.configure({ client = client, ttl = 30, renew_interval = 29 })
    lock.acquire('snapshot')

    t.assert_equals(lock.status().renew_interval, 29)

    now = now + 0.5

    t.assert_equals(lock.holds('snapshot'), true)

    now = now + 0.5

    t.assert_equals(lock.holds('snapshot'), false)
end

--- Кладёт в хранилище замок соседа: он занял ключ, пока срок держания
--- этого узла шёл к концу.
local function neighbour_takes(key)
    state.keys[key] = {
        value = json.encode({ identity = 'storage-002-a', operation = 'snapshot' }),
        revision = 9,
        lease = 'lease-соседа',
    }
end

g.test_release_after_the_term_leaves_the_neighbours_key = function()
    -- Срок держания вышел, и хранилище вправе было отдать замок соседу:
    -- удалить ключ по имени значило бы снять его замок. Своя аренда
    -- отзывается — ключи на ней хранилище снимет само.
    lock.acquire('snapshot')

    local ours = state.keys['/app/locks/snapshot'].lease

    now = now + TTL
    neighbour_takes('/app/locks/snapshot')
    state.calls = {}

    t.assert_equals(lock.release('snapshot'), true)

    t.assert_equals(state.keys['/app/locks/snapshot'].lease, 'lease-соседа')
    t.assert_equals(state.calls, { { op = 'lease_revoke', lease = ours } })
end

g.test_stale_hold_loses_to_the_neighbour = function()
    -- Узел потерял замок молча: продления не доходили. Новая попытка
    -- проигрывает соседу, занявшему ключ, а не снимает его замок.
    lock.acquire('snapshot')

    now = now + TTL
    neighbour_takes('/app/locks/snapshot')

    local taken, err = lock.acquire('snapshot')

    t.assert_equals(taken, false)
    t.assert_equals(err, 'замок занят: snapshot')
    t.assert_equals(lock.owner('snapshot').identity, 'storage-002-a')
end

g.test_guarded_body_refusal_is_passed_on = function()
    -- Отказ тела — та же пара, что и отказ замка: потерянная причина
    -- превратила бы неудачу операции в «ничего не вернула».
    local answer, err = lock.guarded('snapshot', function()
        return nil, 'диск кончился'
    end)

    t.assert_equals(answer, nil)
    t.assert_equals(err, 'диск кончился')
    t.assert_equals(lock.holds('snapshot'), false, 'замок отпущен')
end

g.test_skipped_operation_is_written_down = function()
    -- Запись говорит «не взят», а не «занят»: причина бывает и отказом
    -- хранилища, и выдавать одну за другую нельзя.
    local journal = helper.capture_log()

    journal.forget()

    state.behaviour.lease_grant_error = 'хранилище молчит'

    lock.guarded('snapshot', function() end)

    local record = journal.find('операция пропущена')

    journal.release()

    t.assert_str_contains(record.line, 'операция пропущена: замок не взят')
    t.assert_str_contains(record.line, 'хранилище молчит')
end
