<?php

// Отключаем лишний оверхед Битрикса, чтобы не спамить в статистику и аякс-агенты
define('STOP_STATISTICS', true);
define('NO_KEEP_STATISTIC', true);
define('NO_AGENT_STATISTIC', true);
define('NO_AGENT_CHECK', true);
define('BX_SECURITY_SESSION_READONLY', true);

// Размер пачки для транзакции, чтобы не вешать InnoDB
const IMPORT_TX_CHUNK = 500; 
const IMPORT_LOG_DIR = '/tools/division_update_logs/';
const IMPORT_LOG_TTL_DAYS = 30;

$json = file_get_contents('php://input');
// Сначала проверяем json (только потом подключаем ядро_
if ($json === false || $json === '') {
	respond(['RESULT' => 'error']);
}

$packetBytes = strlen($json);
$arInput = json_decode($json, true);

// Сразу чистим сырую строку из памяти, чтобы не дублировать массив
unset($json); 

if (json_last_error() !== JSON_ERROR_NONE) {
	logImport(
		'отклонён пакет: некорректный json, байт=' . $packetBytes
		. ', ошибка=' . json_last_error_msg()
	);
	respond(['RESULT' => 'error']);
}

if (!is_array($arInput)) {
	logImport('отклонён пакет: ожидался объект логин=код, байт=' . $packetBytes);
	respond(['RESULT' => 'error']);
}
//подключаем ядро
require_once($_SERVER['DOCUMENT_ROOT'] . '/bitrix/modules/main/include/prolog_before.php');

// Дропаем лок сессии сразу после старта, чтобы параллельные хиты от 1С не выстраивались в очередь
if (session_status() === PHP_SESSION_ACTIVE) {
	session_write_close();
}

// Защита от дефолтных 30 секунд PHP на больших объемах
@set_time_limit(0); 


$startedAt = microtime(true);
$cntAll = count($arInput);

logImport(
	'интеграция началась ' . date('d.m.Y H:i:s')
	. ' пакет=' . $cntAll
	. ' байт=' . $packetBytes
);

$helper = new \WebIT\Helpers\DivisionUpdaterHelper();
$connection = \Bitrix\Main\Application::getConnection();

try {
    // пробуем включить режим для оптимизации массовых обновлений 
    // избавляемся от Gap Locks (блокировок промежутков), чтобы минимизировать дедлоки
	$connection->queryExecute('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED');
} catch (\Throwable $e) {
	logImport('не удалось сменить isolation level: ' . $e->getMessage());
}

$chunk = [];

try {
    // Читаем данные, фрмируем пакет для записи
	foreach ($arInput as $login => $code) {
		$chunk[$login] = $code;
        // Вычищаем память на лету по мере обхода массива
		unset($arInput[$login]); 
        
        //Как только пакет наполнился, деаем запись пакетных данных
		if (count($chunk) >= IMPORT_TX_CHUNK) {

			saveChunk($helper, $connection, $chunk);
            // Сбрасываем чанк после успешного коммита
			$chunk = []; 
		}
	}
    #Запись последнего пакета
	if ($chunk) {
		saveChunk($helper, $connection, $chunk);
		$chunk = [];
	}
} catch (\Throwable $e) {
	logImport(
		'интеграция прервана ' . date('d.m.Y H:i:s')
		. ' ошибка=' . $e->getMessage()
	);
	respond(['RESULT' => 'error']);
}

$ms = (int) round((microtime(true) - $startedAt) * 1000);
logImport(
	'интеграция закончилась ' . date('d.m.Y H:i:s')
	. ' пакет=' . $cntAll
	. ' мс=' . $ms
);

respond([
	'ALL' => $cntAll,
	'RESULT' => 'success',
]);

/**
 * Обертка для атомарной записи чанка с механизмом повторных попыток (Retry Pattern)
 */
function saveChunk($helper, $connection, array $rows)
{
	$attempts = 0;

	while (true) {
		$connection->startTransaction();

		try {
			foreach ($rows as $login => $code) {
				$helper->addCodesBy1C([
					'UF_LOGIN' => $login,
					'UF_CODE' => $code,
				]);
			}

			$connection->commitTransaction();
			return;
		} catch (\Throwable $e) {
            // При любой ошибке тушим транзакцию чанка. Фейл роллбэка гасим, чтобы не забивать исходный эксепшн
			try {
				$connection->rollbackTransaction();
			} catch (\Throwable $rollbackError) {
			}

			$attempts++;
            // Если упали по тяжелой ошибке (синтаксис, нет таблицы) или кончились лимиты ретраев — прокидываем наверх
			if ($attempts >= 3 || !isTransientDbError($e)) {
				throw $e;
			}

            // Экспоненциальная пауза перед ретраем (200мс * номер попытки), даем БД разгрести блокировки
			usleep(200000 * $attempts);
		}
	}
}

/**
 * Проверяем, является ли ошибка временным затупом БД (дедлок или таймаут блокировки)
 */
function isTransientDbError(\Throwable $e)
{
	$message = $e->getMessage();

	return stripos($message, 'deadlock') !== false
		|| stripos($message, 'lock wait timeout') !== false
		|| strpos($message, '1213') !== false
		|| strpos($message, '1205') !== false;
}

/**
 * Агрегированный логгер: один день — один файл
 */
function logImport($message)
{
	$baseDir = rtrim((string) $_SERVER['DOCUMENT_ROOT'], '/\\') . IMPORT_LOG_DIR;
	$logDir = $baseDir . date('Y-m-d') . '/';

	if (!is_dir($logDir) && !@mkdir($logDir, 0777, true) && !is_dir($logDir)) {
		return;
	}

	cleanupOldLogDirsOnce($baseDir); // Запускаем сборщик мусора

	$line = '[' . date('Y-m-d H:i:s') . '] ' . $message . "\n";
	@file_put_contents($logDir . 'import.log', $line, FILE_APPEND | LOCK_EX); // LOCK_EX обязателен для конкурентных хитов от 1С
}

/**
 * Изолированный запуск очистки логов не чаще, чем раз в сутки
 */
function cleanupOldLogDirsOnce($baseDir)
{
	static $done = false;

	if ($done) {
		return;
	}

	$done = true;
	$stamp = $baseDir . '.last_cleanup';
	$handle = @fopen($stamp, 'c+');

	if ($handle === false) {
		return;
	}

	try {
        // Используем неблокирующий эксклюзивный лок (LOCK_NB).
        // Если параллельный поток уже убирается, текущий просто идет дальше, не дожидаясь релиза.
		if (!flock($handle, LOCK_EX | LOCK_NB)) {
			return;
		}

		$raw = stream_get_contents($handle);
		$last = is_numeric(trim((string) $raw)) ? (int) trim((string) $raw) : 0;

        // Дропаем выполнение, если сутки с момента последней очистки еще не прошли
		if ($last > time() - 86400) {
			return;
		}

		cleanupOldLogDirs($baseDir);

        // Обновляем метку времени успешной чистки
		rewind($handle);
		ftruncate($handle, 0);
		fwrite($handle, (string) time());
		fflush($handle);
	} finally {
		flock($handle, LOCK_UN);
		fclose($handle);
	}
}

/**
 * Рекурсивная очистка папок логов, которые вышли за рамки TTL
 */
function cleanupOldLogDirs($baseDir)
{
	$threshold = (new DateTimeImmutable('today'))->modify('-' . IMPORT_LOG_TTL_DAYS . ' days');
	$scan = @scandir($baseDir);

	if ($scan === false) {
		return;
	}

	foreach ($scan as $dir) {
        // Интересуют только папки с датами в формате YYYY-MM-DD
		if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $dir)) {
			continue;
		}

		$dirDate = DateTimeImmutable::createFromFormat('!Y-m-d', $dir);
		if (!$dirDate || $dirDate >= $threshold) {
			continue;
		}

		$fullPath = $baseDir . $dir;
		if (!is_dir($fullPath)) {
			continue;
		}

        // Сначала вычищаем файлы внутри директории, затем сносим саму папку
		$files = new DirectoryIterator($fullPath);
		foreach ($files as $file) {
			if ($file->isFile()) {
				@unlink($file->getPathname());
			}
		}
		unset($files);

		@rmdir($fullPath);
	}
}

function respond(array $payload)
{
	echo json_encode($payload);
	die();
}
